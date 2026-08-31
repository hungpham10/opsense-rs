use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::{JoinHandle, spawn};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use futures::FutureExt;
use log::info;

use super::models::{Component, ComponentType, Context, Event, Message, NodeInfo, Outbound};

struct Bootstrap {
    component: RwLock<Arc<dyn Component>>,
    boot: Arc<Semaphore>,
    cancel: CancellationToken,
    signal_tx: watch::Sender<()>,
    signal_rx: watch::Receiver<()>,
    report: mpsc::Sender<Event>,
    receiver: RwLock<Option<mpsc::Receiver<Message>>>,
    fanout: Arc<RwLock<HashMap<usize, mpsc::Sender<Message>>>>,
    broadcast: Arc<RwLock<HashMap<usize, broadcast::Sender<Message>>>>,
    mapping: Arc<RwLock<HashMap<usize, Vec<usize>>>>,
    context: Option<Arc<dyn Context>>,
}

impl Bootstrap {
    // 8 args = wiring runtime (boot/topology/report/context) — mỗi field struct
    // 1 arg, giữ API call site duy nhất ở `Runtime::add_new_nodes` đọc rõ ràng.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        boot: Arc<Semaphore>,
        component: &Arc<dyn Component>,
        rx: mpsc::Receiver<Message>,
        fanout: Arc<RwLock<HashMap<usize, mpsc::Sender<Message>>>>,
        broadcast: Arc<RwLock<HashMap<usize, broadcast::Sender<Message>>>>,
        mapping: Arc<RwLock<HashMap<usize, Vec<usize>>>>,
        report: mpsc::Sender<Event>,
        context: Option<Arc<dyn Context>>,
    ) -> Self {
        let (signal_tx, signal_rx) = watch::channel(());

        Self {
            component: RwLock::new(component.clone()),
            cancel: CancellationToken::new(),
            receiver: RwLock::new(Some(rx)),
            report,
            fanout,
            broadcast,
            mapping,
            signal_rx,
            signal_tx,
            boot,
            context,
        }
    }

    pub fn id(&self) -> Result<String, Error> {
        self.component
            .read()
            .map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Component not readable: {}", error),
                )
            })
            .map(|component| component.id())
    }

    /// Component type of the currently loaded component (for status tooling).
    pub fn component_type(&self) -> ComponentType {
        self.component
            .read()
            .map(|component| component.component_type())
            .unwrap_or(ComponentType::Unknown)
    }

    pub fn reload(&self, component: &Arc<dyn Component>) -> Result<(), Error> {
        let mut lock = self
            .component
            .write()
            .map_err(|_| Error::other("Lock poison"))?;
        *lock = component.clone();

        self.signal_tx.send(()).map_err(|error| {
            Error::new(
                ErrorKind::ConnectionRefused,
                format!("Component not readable: {}", error),
            )
        })?;
        Ok(())
    }

    pub fn stop(&self) -> Result<(), Error> {
        if self.cancel.is_cancelled() {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "Component has been closed",
            ));
        }

        self.cancel.cancel();
        Ok(())
    }

    pub fn compare(&self, component: &Arc<dyn Component>) -> Result<bool, Error> {
        Ok(self
            .component
            .read()
            .map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("component read error: {:?}", error),
                )
            })?
            .compare(component.as_ref()))
    }

    fn get_broadcast(&self, id: usize) -> Result<broadcast::Sender<Message>, Error> {
        Ok(self
            .broadcast
            .read()
            .map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("component read error: {:?}", error),
                )
            })?
            .get(&id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("not found broadcaster for {id}"),
                )
            })?
            .clone())
    }

    fn get_senders(&self, id: usize) -> Result<Vec<mpsc::Sender<Message>>, Error> {
        let mut txs = Vec::new();

        let outputs = self
            .mapping
            .read()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, format!("Mapping read error: {}", e)))?
            .get(&id)
            .cloned()
            .unwrap_or_else(Vec::new);

        let fanout = self
            .fanout
            .read()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, format!("Fanout read error: {}", e)))?;

        for output_id in outputs {
            if let Some(sender) = fanout.get(&output_id) {
                if sender.is_closed() {
                    return Err(Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Sender {} has been closed unexpectedly", output_id),
                    ));
                }

                txs.push(sender.clone());
            } else {
                return Err(Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Sender not found for downstream node {}", output_id),
                ));
            }
        }

        Ok(txs)
    }

    async fn execute(&self, id: usize) -> Result<(), Error> {
        let _permit = self
            .boot
            .acquire()
            .await
            .map_err(|_| Error::new(ErrorKind::Interrupted, "Boot interrupted"))?;

        info!("Component with id {} is on running", id);

        let mut signal = self.signal_rx.clone();
        let cancel = self.cancel.clone();

        let mut rx = {
            let mut rx_guard = self
                .receiver
                .write()
                .map_err(|_| Error::other("Lock poisoned"))?;

            rx_guard
                .take()
                .ok_or_else(|| Error::other("No receiver available"))?
        };

        if rx.is_closed() {
            return Err(Error::new(
                ErrorKind::BrokenPipe,
                format!("Receiver {} has been closed unexpectedly", id),
            ));
        }

        // Cached per iteration: the graph only changes through `reload()`,
        // which fires the watch signal — so panic/error retries reuse the
        // same outbound wiring and only a reload pays for the refetch.
        type OutboundParts = (
            Arc<dyn Component>,
            mpsc::Sender<Event>,
            Vec<mpsc::Sender<Message>>,
            Option<broadcast::Sender<Message>>,
        );
        let mut cached: Option<OutboundParts> = None;
        loop {
            let (component, report, txs, brc) = match &cached {
                Some(parts) => parts,
                None => {
                    let component = self
                        .component
                        .read()
                        .map_err(|error| {
                            Error::new(
                                ErrorKind::BrokenPipe,
                                format!("Failed to read `component`: {}", error),
                            )
                        })?
                        .clone();
                    let report = self.report.clone();
                    let txs = self.get_senders(id)?;
                    let brc = if component.component_type() == ComponentType::Output {
                        Some(self.get_broadcast(id)?)
                    } else {
                        None
                    };
                    cached.insert((component, report, txs, brc))
                }
            };
            let run_in_future = AssertUnwindSafe(component.run(
                id,
                &mut rx,
                Outbound {
                    streams: txs.clone(),
                    broadcast: brc.clone(),
                    event: report.clone(),
                    ctx: self.context.clone(),
                },
            ))
            .catch_unwind();

            tokio::select! {
                panic_res = run_in_future => {
                    match panic_res {
                        Ok(Ok(())) => return Ok(()),
                        Ok(Err(issue)) => {
                            report.send(Event::Major((id, issue)))
                                .await
                                .map_err(|error| Error::new(
                                    ErrorKind::BrokenPipe,
                                    format!(
                                        "Failed to send issue: {}",
                                        error,
                                    ),
                                ))?;
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        Err(panic) => {
                            report.send(Event::Major((
                                    id,
                                    Error::new(
                                        ErrorKind::BrokenPipe,
                                        format!("Panic at node {}:\n {:?}", id, panic),
                                    ),
                                )))
                                .await
                                .map_err(|error| Error::new(
                                    ErrorKind::BrokenPipe,
                                    format!(
                                        "Failed to send issue: {}",
                                        error,
                                    ),
                                ))?;
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }

                _ = cancel.cancelled() => {
                    return Ok(());
                }

                _ = signal.changed() => {
                    // Reload happened: drop the cached wiring so the next
                    // iteration picks up the new component/senders.
                    cached = None;
                    continue;
                }
            }
        }
    }
}

/// Counts how often a node had to wait for its downstream bounded channel to
/// drain (backpressure). A nonzero value means the pipeline is producing
/// faster than a consumer can keep up; it is a measurement only — it does not
/// change the blocking `send` behaviour (the bound itself is the backpressure).
static CHANNEL_FULL_WAITS: AtomicU64 = AtomicU64::new(0);

/// Number of times a data send has been delayed by a full downstream channel
/// (see [`CHANNEL_FULL_WAITS`]). Exposed for status/diagnostics.
#[must_use]
pub fn channel_full_waits() -> u64 {
    CHANNEL_FULL_WAITS.load(Ordering::Relaxed)
}

pub struct Runtime {
    // @NOTE: idea
    //
    // outputs: vec[vec[int]]
    // nodes: HashMap[String, int]
    //
    // a - a -  a
    //   \     /
    //    a - a
    //
    // Base on graph schema above, we can think about incremental
    // validation where when we change anything, the Runtime will easily
    // detect whether or not the graph is broken and cannot stream any
    // more.

    // @NOTE: runtime management
    broadcasts: Arc<RwLock<HashMap<usize, broadcast::Sender<Message>>>>,
    senders: Arc<RwLock<HashMap<usize, mpsc::Sender<Message>>>>,
    boot: Arc<Semaphore>,
    tasks: RwLock<HashMap<usize, JoinHandle<Result<(), Error>>>>,
    bootstraps: RwLock<HashMap<usize, Arc<Bootstrap>>>,
    report_tx: mpsc::Sender<Event>,
    report_rx: Option<mpsc::Receiver<Event>>,
    is_started: bool,

    /// Shared application context injected into every Component.
    context: Option<Arc<dyn Context>>,

    // @NOTE: topology management
    sources: RwLock<HashMap<usize, Vec<usize>>>,
    sinks: Arc<RwLock<HashMap<usize, Vec<usize>>>>,
    inputs: RwLock<HashSet<usize>>,
    outputs: RwLock<HashSet<usize>>,
    nodes: RwLock<HashMap<String, usize>>,
    inc: AtomicUsize,

    /// Bumped after every successful [`Runtime::reload`]; [`Runtime::topology`]
    /// serves its snapshot from cache until this moves again.
    generation: AtomicU64,
    /// `(generation, snapshot)` — status tools poll this often, rebuilding the
    /// full NodeInfo vec per call made it the hottest read path.
    topology_cache: std::sync::Mutex<Option<(u64, std::sync::Arc<Vec<NodeInfo>>)>>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub fn new() -> Self {
        let (report_tx, report_rx) = mpsc::channel(100);
        let report_rx = Some(report_rx);
        Self {
            // @NOTE: declare topology
            sources: RwLock::new(HashMap::new()),
            sinks: Arc::new(RwLock::new(HashMap::new())),
            nodes: RwLock::new(HashMap::new()),
            inputs: RwLock::new(HashSet::new()),
            outputs: RwLock::new(HashSet::new()),
            inc: AtomicUsize::new(0),
            is_started: false,
            report_tx,
            report_rx,

            // @NOTE: declare runtime self-management
            boot: Arc::new(Semaphore::new(0)),
            tasks: RwLock::new(HashMap::new()),
            senders: Arc::new(RwLock::new(HashMap::new())),
            broadcasts: Arc::new(RwLock::new(HashMap::new())),
            bootstraps: RwLock::new(HashMap::new()),

            // @NOTE: shared context (injected before start)
            context: None,

            // @NOTE: topology snapshot cache
            generation: AtomicU64::new(1),
            topology_cache: std::sync::Mutex::new(None),
        }
    }

    /// Inject shared application context that flows to every Component
    /// via `Outbound.ctx`. Must be called before `start()`.
    pub fn set_context(&mut self, ctx: Arc<dyn Context>) {
        self.context = Some(ctx);
    }

    pub fn index(&self, id: String) -> Result<usize, Error> {
        let nodes = self.nodes.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Failed to read from nodes: {}", error),
            )
        })?;

        Ok(*nodes
            .get(&id)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, format!("Not found `{id}`")))?)
    }

    /// Snapshot of every node in the pipeline: type, links and run state.
    ///
    /// Backing data for status/monitoring tools (e.g. MCP `opsense_status`).
    /// Rebuilt only when [`Runtime::reload`] bumps the generation counter —
    /// repeated status calls read one cached snapshot instead of locking all
    /// five maps every time.
    pub fn topology(&self) -> Vec<NodeInfo> {
        let generation = self.generation.load(Ordering::Relaxed);
        if let Ok(cache) = self.topology_cache.lock()
            && let Some((cached_gen, snapshot)) = cache.as_ref()
            && *cached_gen == generation
        {
            return snapshot.as_ref().clone();
        }
        let snapshot = std::sync::Arc::new(self.build_topology());
        if let Ok(mut cache) = self.topology_cache.lock() {
            // Only publish if no reload slipped in while we were building;
            // a stale snapshot must never outlive its generation.
            if self.generation.load(Ordering::Relaxed) == generation {
                *cache = Some((generation, std::sync::Arc::clone(&snapshot)));
            }
        }
        snapshot.as_ref().clone()
    }

    fn build_topology(&self) -> Vec<NodeInfo> {
        let (Ok(nodes), Ok(bootstraps), Ok(tasks), Ok(sources), Ok(sinks)) = (
            self.nodes.read(),
            self.bootstraps.read(),
            self.tasks.read(),
            self.sources.read(),
            self.sinks.read(),
        ) else {
            return Vec::new();
        };

        let idx_to_id: HashMap<usize, &String> = nodes.iter().map(|(id, idx)| (*idx, id)).collect();
        let names = |idxs: &Vec<usize>| -> Vec<String> {
            idxs.iter()
                .filter_map(|i| idx_to_id.get(i).map(|s| (*s).clone()))
                .collect()
        };

        nodes
            .iter()
            .filter_map(|(id, idx)| {
                let bootstrap = bootstraps.get(idx)?;
                Some(NodeInfo {
                    id: id.clone(),
                    component_type: bootstrap.component_type().to_string(),
                    inputs: sources.get(idx).map(&names).unwrap_or_default(),
                    outputs: sinks.get(idx).map(&names).unwrap_or_default(),
                    running: tasks.contains_key(idx),
                })
            })
            .collect()
    }

    pub async fn inject(&self, id: String, msg: Message) -> Result<(), Error> {
        let sender = {
            let nodes = self
                .nodes
                .read()
                .map_err(|e| Error::new(ErrorKind::BrokenPipe, format!("Nodes lock error: {e}")))?;
            let inputs = self.inputs.read().map_err(|e| {
                Error::new(ErrorKind::BrokenPipe, format!("Inputs lock error: {e}"))
            })?;
            let senders = self.senders.read().map_err(|e| {
                Error::new(ErrorKind::BrokenPipe, format!("Senders lock error: {e}"))
            })?;

            let idx = nodes.get(&id).ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Not found node with id {id}"),
                )
            })?;

            if inputs.contains(idx) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Node {id} must be a source node"),
                ));
            }

            senders.get(idx).cloned().ok_or_else(|| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Node {id} doesn't have any senders"),
                )
            })
        }?;

        let wait_start = Instant::now();
        sender.send(msg).await.map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Send data to node {id} failed: {error}"),
            )
        })?;
        // A full-channel await is the natural backpressure point; only record
        // the stall when it actually cost more than 100ms.
        let waited = wait_start.elapsed();
        if waited > Duration::from_millis(100) {
            CHANNEL_FULL_WAITS.fetch_add(1, Ordering::Relaxed);
            log::warn!(
                "backpressure: send to node {id} blocked for {:.1?} waiting on a full channel",
                waited
            );
        }

        Ok(())
    }

    pub fn broadcast(&self, id: String) -> Result<broadcast::Receiver<Message>, Error> {
        let idx = self.index(id.clone())?;

        {
            let outputs = self.outputs.read().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Failed to read `outputs` lock: {}", error),
                )
            })?;

            if !outputs.contains(&idx) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "Node '{}' (idx: {}) is not registered as an Output/Sink node.",
                        id, idx
                    ),
                ));
            }
        }

        let mut broadcasts_guard = self.broadcasts.write().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Failed to write `broadcasts` lock: {}", error),
            )
        })?;

        // @NOTE: broadcast must be in realtime
        if !self.is_started || broadcasts_guard.contains_key(&idx) {
            let sender = broadcasts_guard.entry(idx).or_insert_with(|| {
                let (tx, _) = broadcast::channel(1024);
                tx
            });

            Ok(sender.subscribe())
        } else {
            Err(Error::new(
                ErrorKind::BrokenPipe,
                format!(
                    "Failed to create new broadcast channel for {id} because of runtime has been started"
                ),
            ))
        }
    }

    pub fn reload(&self, components: Vec<Arc<dyn Component>>) -> Result<(), Error> {
        let (adds, diffs, dels) = {
            let nodes = self.nodes.read().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Failed reading nodes: {}", error),
                )
            })?;
            let bootstraps = self.bootstraps.read().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Failed reading bootstrap: {:?}", error),
                )
            })?;

            let mut diffs = Vec::new();
            let mut adds = Vec::new();
            let mut dels = bootstraps.keys().collect::<HashSet<_>>();

            for component in &components {
                if let Some(idx) = nodes.get(&component.id()) {
                    let bootstrap = bootstraps.get(idx).ok_or_else(|| {
                        Error::new(
                            ErrorKind::BrokenPipe,
                            format!("Failed to get bootstrap with id {}", idx),
                        )
                    })?;

                    bootstrap.compare(component).map(|is_same| {
                        if !is_same {
                            diffs.push(component);
                        }
                    })?;

                    dels.remove(&idx);
                } else {
                    adds.push(component);
                }
            }

            (adds, diffs, dels.into_iter().copied().collect::<Vec<_>>())
        };

        self.validate_if_adding_new_nodes(&adds, &diffs)?;
        self.validate_if_changing_nodes(&diffs, &adds)?;
        self.validate_if_remove_outdated_nodes(&dels, &adds, &diffs)?;
        self.validate_no_cycles(&adds, &diffs, &dels)?;

        self.add_new_nodes(&adds)?;
        self.configure_links_after_adding_new_nodes(&adds)?;

        self.update_changing_nodes(&diffs)?;
        self.configure_links_after_changing_nodes(&diffs)?;

        self.remove_oudated_nodes(&dels)?;
        self.configure_links_after_remove_oudated_nodes(&dels)?;

        if self.is_started {
            let permit_count = self
                .bootstraps
                .read()
                .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?
                .len();

            self.boot.add_permits(permit_count);
        }

        // Invalidate the topology snapshot exactly once per applied change.
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn start<F, Fut>(&mut self, handler: F) -> Result<JoinHandle<()>, Error>
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.is_started {
            return Err(Error::new(ErrorKind::BrokenPipe, "already started"));
        }

        let mut rx = self
            .report_rx
            .take()
            .ok_or_else(|| Error::other("receiver already taken"))?;

        let task_handler = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                handler(event).await;
            }
        });

        let permit_count = self
            .bootstraps
            .read()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?
            .len();

        self.boot.add_permits(permit_count);
        self.is_started = true;
        Ok(task_handler)
    }

    pub fn stop(&self) -> Result<(), Error> {
        self.reload(Vec::new())
    }

    pub async fn wait_for_shutdown(&self) -> Result<(), Error> {
        let tasks: Vec<_> = {
            let mut tasks_guard = self.tasks.write().unwrap();
            tasks_guard.drain().map(|(_, handle)| handle).collect()
        };

        for handle in tasks {
            let _ = handle.await;
        }

        info!("All component tasks have been shut down");
        Ok(())
    }

    fn add_new_nodes(&self, adds: &Vec<&Arc<dyn Component>>) -> Result<(), Error> {
        for component in adds {
            let idx = self.inc.fetch_add(1, Ordering::SeqCst);
            let (tx_data, rx_data) = mpsc::channel::<Message>(1024);

            self.nodes
                .write()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Failed to write to nodes: {}", error),
                    )
                })?
                .entry(component.id())
                .or_insert(idx);

            if component.component_type() == ComponentType::Input {
                self.inputs
                    .write()
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::BrokenPipe,
                            format!("Failed to write to nodes: {}", error),
                        )
                    })?
                    .insert(idx);
            }

            if component.component_type() == ComponentType::Output {
                self.outputs
                    .write()
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::BrokenPipe,
                            format!("Failed to write to nodes: {}", error),
                        )
                    })?
                    .insert(idx);

                self.broadcasts
                    .write()
                    .map_err(|error| {
                        Error::new(
                            ErrorKind::BrokenPipe,
                            format!("Failed to write to nodes: {}", error),
                        )
                    })?
                    .entry(idx)
                    .or_insert_with(|| {
                        let (tx, _) = broadcast::channel::<Message>(1024);
                        tx
                    });
            }

            self.senders
                .write()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Failed to write to senders: {}", error),
                    )
                })?
                .entry(idx)
                .or_insert(tx_data.clone());

            let bootstrap = Arc::new(Bootstrap::new(
                self.boot.clone(),
                component,
                rx_data,
                self.senders.clone(),
                self.broadcasts.clone(),
                self.sinks.clone(),
                self.report_tx.clone(),
                self.context.clone(),
            ));

            self.bootstraps
                .write()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Fail writing bootstrap {:?}", error),
                    )
                })?
                .entry(idx)
                .or_insert(bootstrap.clone());

            info!("Component {} with id {} is starting", component.id(), idx);

            // Eagerly call pre_run before spawning so shared resources (e.g.
            // stations) are registered before any task polling or select! fires.
            // `add_new_nodes` is sync but the surrounding `reload` is invoked
            // from an async context, so we drive the future on a separate OS
            // thread to avoid "Cannot start a runtime from within a runtime"
            // from `Handle::block_on` on a worker thread.
            info!("engine.add_new_nodes: invoking pre_run for component {}", component.id());
            let (tx_done, rx_done) = std::sync::mpsc::channel::<Result<(), String>>();
            // Deref the reference to get the Arc, then clone the Arc (cheap refcount bump).
            let comp: Arc<dyn Component> = Arc::clone(*component);
            std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = tx_done.send(Err(format!(
                            "pre_run: failed to build runtime: {}",
                            e
                        )));
                        return;
                    }
                };
                let result = rt.block_on(async move {
                    comp.pre_run().await.map_err(|e| e.to_string())
                });
                let _ = tx_done.send(result);
            });
            match rx_done.recv() {
                Ok(Ok(())) => {
                    info!("engine.add_new_nodes: pre_run completed ok for {}", component.id());
                }
                Ok(Err(e)) => {
                    return Err(Error::other(
                        format!("pre_run failed for {}: {}", component.id(), e),
                    ));
                }
                Err(e) => {
                    return Err(Error::other(
                        format!("pre_run thread dropped for {}: {}", component.id(), e),
                    ));
                }
            }

            self.tasks
                .write()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Failed to write to tasks: {}", error),
                    )
                })?
                .insert(
                    idx,
                    spawn(async move {
                        let ret = bootstrap.execute(idx).await;
                        if let Err(error) = ret {
                            info!("Component with id {} is closed: {}", idx, error);
                            Err(error)
                        } else {
                            Ok(())
                        }
                    }),
                );
        }

        Ok(())
    }

    fn validate_if_adding_new_nodes(
        &self,
        adds: &Vec<&Arc<dyn Component>>,
        diffs: &Vec<&Arc<dyn Component>>,
    ) -> Result<(), Error> {
        let nodes = self.nodes.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Failed to write to nodes: {}", error),
            )
        })?;
        let will_add = adds
            .iter()
            .enumerate()
            .map(|(idx, component)| (component.id(), idx))
            .collect::<HashMap<_, _>>();
        let will_be_linked = diffs
            .iter()
            .filter_map(|component| component.get_inputs())
            .flatten()
            .cloned()
            .collect::<HashSet<_>>();

        let mut dead_nodes = will_add.clone();

        for component in adds {
            if !matches!(
                component.component_type(),
                ComponentType::Source | ComponentType::Input
            ) {
                if let Some(inputs) = component.get_inputs() {
                    if inputs.is_empty() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "Node '{}' requires to have at least one input",
                                component.id(),
                            ),
                        ));
                    }
                } else {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "Node '{}' requires to not return None when listing inputs",
                            component.id(),
                        ),
                    ));
                }
            }

            if let Some(inputs) = component.get_inputs() {
                for input in inputs {
                    let exists_in_current = nodes.contains_key(input);
                    let exists_in_adds = will_add.contains_key(input);

                    if !exists_in_current && !exists_in_adds {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "Validation Failed: Node '{}' requires input '{}' but it's not found.",
                                component.id(),
                                input,
                            ),
                        ));
                    }

                    if let Some(&idx) = will_add.get(input) {
                        if matches!(
                            adds[idx].component_type(),
                            ComponentType::Sink | ComponentType::Output
                        ) {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                format!("Node {} must not be Sink or Output", adds[idx].id()),
                            ));
                        }

                        dead_nodes.remove(input);
                    }
                }
            } else if !matches!(
                component.component_type(),
                ComponentType::Source | ComponentType::Input
            ) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "Node {} must be Source or Input, not {}",
                        component.id(),
                        component.component_type(),
                    ),
                ));
            }
        }

        for (_, idx) in dead_nodes {
            if !matches!(
                adds[idx].component_type(),
                ComponentType::Sink | ComponentType::Output
            ) && !adds[idx].is_terminal()
                && !will_be_linked.contains(&adds[idx].id())
            {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "{} {} must have connect with another nodes",
                        adds[idx].component_type(),
                        adds[idx].id(),
                    ),
                ));
            }
        }

        Ok(())
    }

    fn configure_links_after_adding_new_nodes(
        &self,
        adds: &Vec<&Arc<dyn Component>>,
    ) -> Result<(), Error> {
        let mut sources_map = self.sources.write().unwrap();
        let mut sinks_map = self.sinks.write().unwrap();
        let nodes = self.nodes.read().unwrap();

        for component in adds {
            let current_idx = *nodes.get(&component.id()).unwrap();

            if let Some(inputs) = component.get_inputs() {
                let mut input_indices = Vec::new();

                for input_name in inputs {
                    if let Some(&source_idx) = nodes.get(input_name) {
                        let outs = sinks_map.entry(source_idx).or_default();
                        if !outs.contains(&current_idx) {
                            outs.push(current_idx);
                        }

                        if !input_indices.contains(&source_idx) {
                            input_indices.push(source_idx);
                        }
                    }
                }
                sources_map.insert(current_idx, input_indices);
            }
        }
        Ok(())
    }

    fn validate_if_changing_nodes(
        &self,
        diffs: &Vec<&Arc<dyn Component>>,
        adds: &Vec<&Arc<dyn Component>>,
    ) -> Result<(), Error> {
        let nodes = self.nodes.read().unwrap();
        let add_ids: HashSet<String> = adds.iter().map(|c| c.id()).collect();

        for component in diffs {
            if let Some(inputs) = component.get_inputs() {
                for input_id in inputs {
                    if !nodes.contains_key(input_id) && !add_ids.contains(input_id) {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "Node {} wants to receive from non-existent node {}",
                                component.id(),
                                input_id,
                            ),
                        ));
                    }

                    if input_id == &component.id() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Self-loop is not allowed",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Reject dependency cycles in the graph this reload is about to install.
    ///
    /// The final node set is "existing minus removals plus additions/changes";
    /// edges come from each node's declared inputs — component objects where
    /// the caller provided them, otherwise the already-wired link table.
    /// Without this, an `opsense_edit` could wire `a → b → a` and have every
    /// tick chase its own tail forever.
    fn validate_no_cycles(
        &self,
        adds: &Vec<&Arc<dyn Component>>,
        diffs: &Vec<&Arc<dyn Component>>,
        dels: &[usize],
    ) -> Result<(), Error> {
        let nodes = self.nodes.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Failed reading nodes: {}", error),
            )
        })?;
        let links = self.sources.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Failed reading sources: {}", error),
            )
        })?;

        let id_by_idx: HashMap<usize, String> =
            nodes.iter().map(|(id, idx)| (*idx, id.clone())).collect();

        let mut edges: HashMap<String, Vec<String>> = HashMap::new();
        // Survivors keep their wired inputs…
        for (id, idx) in nodes.iter() {
            if dels.contains(idx) {
                continue;
            }
            let wired = links
                .get(idx)
                .into_iter()
                .flatten()
                .filter_map(|in_idx| id_by_idx.get(in_idx).cloned());
            edges.insert(id.clone(), wired.collect());
        }
        // …adds and diffs override with whatever they declare now.
        for component in adds.iter().chain(diffs.iter()) {
            edges.insert(
                component.id(),
                component.get_inputs().cloned().unwrap_or_default(),
            );
        }

        match find_cycle(&edges) {
            Some(cycle) => Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Validation Failed: pipeline graph contains a cycle: {}",
                    cycle.join(" -> ")
                ),
            )),
            None => Ok(()),
        }
    }

    fn update_changing_nodes(&self, diffs: &Vec<&Arc<dyn Component>>) -> Result<(), Error> {
        for component in diffs {
            let idx = *self
                .nodes
                .read()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Fail writing bootstrap {:?}", error),
                    )
                })?
                .get(&component.id())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Failed to get id of node {}", component.id()),
                    )
                })?;

            self.bootstraps
                .write()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Fail writing bootstrap {:?}", error),
                    )
                })?
                .get(&idx)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Not found component with id {}", component.id()),
                    )
                })?
                .reload(component)?;
        }

        Ok(())
    }

    fn configure_links_after_changing_nodes(
        &self,
        diffs: &Vec<&Arc<dyn Component>>,
    ) -> Result<(), Error> {
        let mut sources_map = self.sources.write().unwrap();
        let mut sinks_map = self.sinks.write().unwrap();
        let nodes = self.nodes.read().unwrap();

        for component in diffs {
            let current_id = component.id();
            let current_idx = *nodes.get(&current_id).expect("Bug: Diff node not found");

            let new_input_indices: Vec<usize> = component
                .get_inputs()
                .map(|v| v.iter().filter_map(|id| nodes.get(id).cloned()).collect())
                .unwrap_or_default();

            let old_input_indices = sources_map.get(&current_idx).cloned().unwrap_or_default();

            for old_source_idx in &old_input_indices {
                if !new_input_indices.contains(old_source_idx)
                    && let Some(outs) = sinks_map.get_mut(old_source_idx)
                {
                    outs.retain(|&idx| idx != current_idx);
                }
            }

            for &new_source_idx in &new_input_indices {
                if !old_input_indices.contains(&new_source_idx) {
                    let outs = sinks_map.entry(new_source_idx).or_default();
                    if !outs.contains(&current_idx) {
                        outs.push(current_idx);
                    }
                }
            }

            sources_map.insert(current_idx, new_input_indices);
        }
        Ok(())
    }

    fn validate_if_remove_outdated_nodes(
        &self,
        dels: &Vec<usize>,
        adds: &Vec<&Arc<dyn Component>>,
        diffs: &Vec<&Arc<dyn Component>>,
    ) -> Result<(), Error> {
        let nodes = self.nodes.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Failed to write to nodes: {}", error),
            )
        })?;
        let inputs = self.sources.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Fail reading inputs {:?}", error),
            )
        })?;
        let outputs = self.sinks.read().map_err(|error| {
            Error::new(
                ErrorKind::BrokenPipe,
                format!("Fail reading outputs {:?}", error),
            )
        })?;
        let will_change_input = diffs
            .iter()
            .map(|component| {
                let node_idx = nodes
                    .get(&component.id())
                    .cloned()
                    .expect("Never reach to this point or this is bug");

                let input_indices = component
                    .get_inputs()
                    .map(|v| {
                        v.iter()
                            .filter_map(|id| nodes.get(id).cloned())
                            .collect::<HashSet<_>>()
                    })
                    .unwrap_or_default();

                (node_idx, input_indices)
            })
            .collect::<HashMap<_, _>>();
        let will_delete = dels.iter().collect::<HashSet<_>>();
        let will_be_linked = adds
            .iter()
            .filter_map(|component| component.get_inputs())
            .flatten()
            .filter_map(|id| nodes.get(id))
            .cloned()
            .collect::<HashSet<_>>();

        for id_of_dead_node in dels {
            if will_be_linked.contains(id_of_dead_node) {
                // @TODO: mapping id to node name

                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "You are adding new node to receive data from dead node {}",
                        id_of_dead_node,
                    ),
                ));
            }

            let mut sent_by_nodes = if let Some(input) = inputs.get(id_of_dead_node) {
                input.iter().collect::<HashSet<_>>()
            } else {
                HashSet::new()
            };

            if let Some(output) = outputs.get(id_of_dead_node) {
                for receiving_node_id in output {
                    if will_delete.contains(receiving_node_id) {
                        continue;
                    }

                    if let Some(changing_inputs) = will_change_input.get(receiving_node_id) {
                        if changing_inputs.contains(id_of_dead_node) {
                            // @TODO: mapping id to node name

                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                format!(
                                    "Node {} mustn't receive transaction from {}",
                                    receiving_node_id, id_of_dead_node,
                                ),
                            ));
                        }

                        for input in changing_inputs {
                            sent_by_nodes.remove(input);
                        }
                    } else {
                        // @TODO: mapping id to node name
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "Node {} mustn't receive transaction from {}",
                                receiving_node_id, id_of_dead_node,
                            ),
                        ));
                    }
                }
            }

            if !sent_by_nodes.is_empty() {
                for node_id in sent_by_nodes {
                    if let Some(output) = outputs.get(node_id)
                        && output.len() == 1
                        && !will_delete.contains(node_id)
                    {
                        // @TODO: mapping id to node name

                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!("Node {} is on deadline", node_id),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_oudated_nodes(&self, dels: &Vec<usize>) -> Result<(), Error> {
        for id in dels {
            let mut bootstraps = self.bootstraps.write().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Fail writing bootstrap {:?}", error),
                )
            })?;
            let mut tasks = self.tasks.write().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Fail writing bootstrap {:?}", error),
                )
            })?;
            let mut nodes = self.nodes.write().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Fail writing nodes {:?}", error),
                )
            })?;
            let mut inputs = self.inputs.write().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Fail writing inputs {:?}", error),
                )
            })?;
            let mut outputs = self.outputs.write().map_err(|error| {
                Error::new(
                    ErrorKind::BrokenPipe,
                    format!("Fail writing outputs {:?}", error),
                )
            })?;

            let name = bootstraps
                .get(id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Not found component with id {}", id),
                    )
                })?
                .id()
                .map_err(|error| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Fail query node name with id {}: {:?}", id, error),
                    )
                })?;

            bootstraps
                .get(id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("Not found component with id {}", id),
                    )
                })?
                .stop()?;
            bootstraps.remove(id);
            tasks.remove(id);
            inputs.remove(id);
            outputs.remove(id);
            nodes.remove(&name);
        }

        Ok(())
    }

    fn configure_links_after_remove_oudated_nodes(&self, dels: &Vec<usize>) -> Result<(), Error> {
        let mut sources_map = self
            .sources
            .write()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;
        let mut sinks_map = self
            .sinks
            .write()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;
        let mut senders_map = self
            .senders
            .write()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;
        let mut broadcasts_map = self
            .broadcasts
            .write()
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;

        for &id_of_dead_node in dels {
            if let Some(upstream_indices) = sources_map.get(&id_of_dead_node) {
                for &source_idx in upstream_indices {
                    if let Some(outs) = sinks_map.get_mut(&source_idx) {
                        outs.retain(|&idx| idx != id_of_dead_node);
                    }
                }
            }

            sources_map.remove(&id_of_dead_node);
            sinks_map.remove(&id_of_dead_node);
            senders_map.remove(&id_of_dead_node);
            broadcasts_map.remove(&id_of_dead_node);
        }

        Ok(())
    }
}

/// Three-color DFS over `node -> inputs`; returns one loop as a path that
/// starts and ends on the same node, or `None` when the graph is a DAG.
fn find_cycle(edges: &HashMap<String, Vec<String>>) -> Option<Vec<String>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    fn visit(
        node: &str,
        edges: &HashMap<String, Vec<String>>,
        color: &mut HashMap<String, Color>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        color.insert(node.to_string(), Color::Gray);
        path.push(node.to_string());
        for next in edges.get(node).into_iter().flatten() {
            match color.get(next).copied().unwrap_or(Color::White) {
                Color::Gray => {
                    // Back-edge: everything from the first occurrence of
                    // `next` down to here is the loop.
                    let start = path.iter().position(|n| n == next).unwrap_or(0);
                    let mut cycle = path[start..].to_vec();
                    cycle.push(next.clone());
                    return Some(cycle);
                }
                Color::White => {
                    if let Some(cycle) = visit(next, edges, color, path) {
                        return Some(cycle);
                    }
                }
                Color::Black => {}
            }
        }
        path.pop();
        color.insert(node.to_string(), Color::Black);
        None
    }

    let mut color: HashMap<String, Color> = HashMap::new();
    for root in edges.keys() {
        if color.get(root).copied().unwrap_or(Color::White) == Color::White {
            let mut path = Vec::new();
            if let Some(cycle) = visit(root, edges, &mut color, &mut path) {
                return Some(cycle);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::find_cycle;

    fn graph(pairs: &[(&str, &[&str])]) -> std::collections::HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(node, inputs)| {
                (
                    node.to_string(),
                    inputs.iter().map(|s| s.to_string()).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn find_cycle_flags_loops_in_every_shape() {
        // Two-node ping-pong.
        let two = graph(&[("a", &["b"]), ("b", &["a"])]);
        let cycle = find_cycle(&two).expect("two-node cycle must be found");
        assert_eq!(cycle.first(), cycle.last(), "path closes on itself");

        // Three-node ring reachable only from the middle of the walk.
        let three = graph(&[
            ("clock", &[]),
            ("a", &["clock", "c"]),
            ("b", &["a"]),
            ("c", &["b"]),
        ]);
        assert!(find_cycle(&three).is_some());

        // Direct self-loop.
        let self_loop = graph(&[("a", &["a"])]);
        assert_eq!(find_cycle(&self_loop), Some(vec!["a".into(), "a".into()]));
    }

    #[test]
    fn find_cycle_passes_acyclic_graphs() {
        // Diamond: two branches converging on one node — legal.
        let diamond = graph(&[
            ("clock", &[]),
            ("b1", &["clock"]),
            ("b2", &["clock"]),
            ("sink", &["b1", "b2"]),
        ]);
        assert_eq!(find_cycle(&diamond), None);

        // Empty / single-node graphs.
        assert_eq!(find_cycle(&graph(&[])), None);
        assert_eq!(find_cycle(&graph(&[("only", &[])])), None);
    }

    use tokio::sync::broadcast;

    #[tokio::test]
    async fn test_broadcast_auto_resuscitation() {
        // Khởi tạo channel và drop ngay lập tức receiver mặc định đi kèm
        // Hiện tại: 0 Subscriber
        let (tx, rx_default) = broadcast::channel::<String>(10);
        drop(rx_default);

        // ====================================================
        // GIAI ĐOẠN 1: Gửi khi KHÔNG CÓ AI nghe
        // ====================================================
        let msg1 = "Tin nhắn khi vắng khách".to_string();
        let result1 = tx.send(msg1);

        // Kiểm tra: Phải trả về lỗi và lỗi đó phải là SendError dòng Closed
        assert!(result1.is_err(), "Kênh không có ai nghe thì phải báo lỗi");
        if let Err(broadcast::error::SendError(error)) = result1 {
            // Đúng chuẩn lỗi SendError của Tokio khi channel bị rỗng khách
            println!("✓ Giai đoạn 1 đúng: Hệ thống báo lỗi Closed vì subscriber = 0: {error}");
        }

        // ====================================================
        // GIAI ĐOẠN 2: Có 1 Client WebSocket vào subscribe
        // ====================================================
        let mut client_rx = tx.subscribe();

        // Gửi tin nhắn tiếp theo
        let msg2 = "Tin nhắn thời gian thực".to_string();
        let result2 = tx.send(msg2.clone());

        // Kiểm tra: Kênh PHẢI tự hồi sinh, trả về Ok(1) - số 1 là số lượng người nhận
        assert!(
            result2.is_ok(),
            "Kênh phải tự hồi sinh khi có người subscribe!"
        );
        assert_eq!(result2.unwrap(), 1, "Số lượng nhận được tin nhắn phải là 1");

        // Kiểm tra xem client đó có thực sự nhận được dữ liệu không
        let received_msg = client_rx.recv().await.unwrap();
        assert_eq!(received_msg, msg2, "Client phải nhận đúng dữ liệu mới phát");
        println!("✓ Giai đoạn 2 đúng: Kênh tự hồi sinh mượt mà, client nhận data OK!");

        // ====================================================
        // GIAI ĐOẠN 3: Client WebSocket ngắt kết nối (Drop)
        // ====================================================
        drop(client_rx); // Mô phỏng client tắt trình duyệt / mất mạng

        // Gửi tin nhắn tiếp theo lần nữa
        let msg3 = "Tin nhắn sau khi khách rời đi".to_string();
        let result3 = tx.send(msg3);

        // Kiểm tra: Kênh PHẢI tự động quay về trạng thái báo lỗi lỗi
        assert!(result3.is_err(), "Khách đi rồi thì kênh phải báo lỗi tiếp");
        println!("✓ Giai đoạn 3 đúng: Khách đi thì lại báo lỗi Closed, không tốn bộ nhớ cache.");
    }
}
