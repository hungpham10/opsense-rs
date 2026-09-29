use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::io::Error;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

pub enum Event {
    Minor((usize, Error)),
    Major((usize, Error)),
    Panic((usize, Error)),
    /// Node **vừa hồi phục** — chạy lại thành công sau một hoặc nhiều lần lỗi.
    ///
    /// Vì sao cần biến thể riêng: `last_error` lưu **lần lỗi gần nhất**, nên
    /// node đã khoẻ vẫn còn hiện lỗi cũ ⇒ status báo động giả, và người đọc
    /// không biết đã hết chưa. Không thể suy từ "không nhận `Fault` nữa" vì
    /// im lặng cũng là một trạng thái hợp lệ — phải nói rõ.
    Recovered(usize),
    /// Lỗi **đã phân loại** do component tự bắn: mang cả mức độ nên handler
    /// không phải đoán từ message mà gán cấp độ.
    ///
    /// `Major`/`Panic` là của **engine** (chính `run()` trả `Err` hoặc panic) —
    /// engine chỉ thử lại được, không sửa được, nên không phân loại thêm. Còn
    /// `Fault` là lỗi mà component biết rõ hậu quả và thường **đã tự hồi
    /// phục** (`recovered`).
    Fault((usize, Fault)),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(i32)]
pub enum ComponentType {
    Unknown,
    Input,
    Output,
    Source,
    Sink,
    Transform,
}

impl From<i32> for ComponentType {
    fn from(value: i32) -> Self {
        match value {
            1 => ComponentType::Source,
            2 => ComponentType::Sink,
            3 => ComponentType::Transform,
            4 => ComponentType::Input,
            5 => ComponentType::Output,
            _ => ComponentType::Unknown,
        }
    }
}

impl From<String> for ComponentType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Source" => ComponentType::Source,
            "Sink" => ComponentType::Sink,
            "Transform" => ComponentType::Transform,
            "Input" => ComponentType::Input,
            "Output" => ComponentType::Output,
            _ => ComponentType::Unknown,
        }
    }
}

impl Display for ComponentType {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ComponentType::Unknown => write!(f, "Unknown"),
            ComponentType::Source => write!(f, "Source"),
            ComponentType::Sink => write!(f, "Sink"),
            ComponentType::Input => write!(f, "Input"),
            ComponentType::Output => write!(f, "Output"),
            ComponentType::Transform => write!(f, "Transform"),
        }
    }
}

impl<'de> Deserialize<'de> for ComponentType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ComponentType::from(s))
    }
}

impl serde::Serialize for ComponentType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Message {
    pub payload: Value,
}

/// Mức độ lỗi — quyết định hệ thống **tự làm gì**, không chỉ để hiển thị.
///
/// `Event::Minor/Major/Panic` cũ không đủ dùng ở đây: `Minor` là "lỗi bố trí"
/// (batch lệch nối) còn pipeline vẫn chạy, còn các lỗi dưới đây đều **làm node
/// mất chức năng** nhưng khác nhau về cách hồi phục — gộp chung thì không biết
/// nên thử lại hay phải dựng lại.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Lỗi tạm thời, thử lại ở chu kỳ sau. Mạng 5xx, chưa đủ dữ liệu, kernel
    /// rebuild thiếu nến. Giữ nguyên state.
    Transient,
    /// State trong station **hỏng**. Giữ lại thì mắc vĩnh viễn — ví dụ plan
    /// JSON hỏng thì plan rỗng và node không bao giờ đặt lệnh nữa, mà nhìn ra
    /// vẫn "đang chạy". Cần **dựng lại** state từ nguồn sạch.
    Corrupt,
    /// Cấu hình sai, không tự hồi phục được: tên `strategy` không hỗ trợ,
    /// thiếu `params.dag`, URL sai cú pháp. Retry vô ích — nhưng **không được
    /// chết** vì lỗi cấu hình của một node không kéo chết cả pipeline.
    Fatal,
}

/// Một lỗi đã phân loại, kèm hành động hồi phục tự động đã áp dụng (để log và
/// status nói rõ hệ thống **đã làm gì**, không chỉ "có lỗi").
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Fault {
    pub severity: Severity,
    /// Mã ổn định để đếm/gom theo thứ, không phải message tiếng Anh đầy đủ.
    pub code: String,
    pub message: String,
    /// `None` = chưa/làm không được; `Some` = mô tả hành động đã tự áp dụng.
    pub recovered: Option<String>,
}

impl Severity {
    /// Tên ổn định để log và `opsense_status` hiện ra, khớp `serde` rename.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Corrupt => "corrupt",
            Self::Fatal => "fatal",
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Fault {
    /// `severity: code: message` — một dòng đủ để đọc trong log lẫn status.
    pub fn summary(&self) -> String {
        let mut s = format!("{}: {}: {}", self.severity, self.code, self.message);
        if let Some(r) = &self.recovered {
            s.push_str(&format!(" (đã tự hồi phục: {r})"));
        }
        s
    }
    pub fn new(severity: Severity, code: &str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code: code.to_string(),
            message: message.into(),
            recovered: None,
        }
    }

    /// Ghi nhận hành động hồi phục đã áp dụng.
    #[must_use]
    pub fn recovered(mut self, what: impl Into<String>) -> Self {
        self.recovered = Some(what.into());
        self
    }
}

/// Read-only view of one node in the running pipeline, for status tooling.
#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub id: String,
    pub component_type: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub running: bool,
    /// Lỗi gần nhất của node, `None` khi node đang khoẻ.
    ///
    /// Vì sao cần: `run()` của hầu hết component **không** trả `Err` khi hỏng
    /// (script lỗi chỉ `warn!` rồi bỏ batch) nên `Event::Major` của engine không
    /// kích hoạt, và `println!` ở handler mất ngay khi container restart. Hỏi
    /// status mới là cách duy nhất hỏi được node đang chết vì sao.
    pub last_error: Option<Fault>,
    /// Số lần node báo lỗi. Phân biệt "lỗi tĩnh lặp mỗi nến" với "một lần rồi
    /// hết" — cùng một `last_error` nhưng khác sức nặng hoàn toàn.
    pub fault_count: u64,
}

/// Context trait — type-erased container for shared application state.
///
/// The Runtime holds an `Arc<dyn Context>` and injects it into each Component
/// via `Outbound.ctx`. Components that need shared resources (Redis, DB, etc.)
/// downcast this to the concrete type (e.g. `Resolver`).
pub trait Context: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
}

pub struct Outbound {
    pub streams: Vec<mpsc::Sender<Message>>,
    pub broadcast: Option<broadcast::Sender<Message>>,
    pub event: mpsc::Sender<Event>,

    /// Shared application context injected by the Runtime.
    /// Components downcast to access concrete resources.
    pub ctx: Option<Arc<dyn Context>>,
}

pub trait Identify {
    fn id(&self) -> String;
    fn get_inputs(&self) -> Option<&Vec<String>>;
    fn clone_arc(&self) -> Arc<dyn Component>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn component_type(&self) -> ComponentType;
    /// True khi component tự phục vụ dữ liệu của chính nó (vd: đăng ký
    /// station queryable qua MCP/HTTP) và không cần node downstream.
    fn is_terminal(&self) -> bool {
        false
    }
    fn compare(&self, other: &dyn Component) -> bool;
}

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait Component: Identify + Send + Sync + Debug {
    async fn run(
        &self,
        id: usize,
        rx: &mut mpsc::Receiver<Message>,
        tx: Outbound,
    ) -> Result<(), Error>;

    /// Called once during pipeline construction, before the component task is
    /// spawned. Use this for eager registration of shared resources (e.g.
    /// stations) that must be visible before any `run()` polling occurs.
    async fn prepare(&self) -> Result<(), Error> {
        Ok(())
    }
}
