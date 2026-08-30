# Opsense Julia analysis kernel.
#
# Nói framed stdio protocol của opsense-proto, giống hệt kernel Python/echo:
#   CONTROL frame = protobuf `Envelope` (codec tự viết bên dưới — không cần
#   package nào), ARROW frame = một Arrow IPC stream segment.
#
# Dependencies: KHÔNG bắt buộc gì. Có `Arrow.jl` trong depot mặc định (@v1.x)
# sẽ bật đường truyền dataset/DataFrame qua lại; thiếu nó thì health vẫn OK,
# chỉ các thao tác dataset mới báo lỗi rõ ràng.
#
# Chạy bởi launcher Rust (`opsense-kernel-julia`) với `-t 2` để reader chạy
# trên thread riêng; thread chính thực thi code người dùng.
#
# Directives tương thích harness (giống kernel echo/Python):
#   sleep:<ms> | print:<text> | err:<kind>:<message> | df | code Julia thuần
#   (biến `result` được capture về host).

const PROTO_VERSION = UInt32(1)
const KERNEL_NAME = "julia"
const TAG_CONTROL = UInt8(0x01)
const TAG_ARROW = UInt8(0x02)

# Real protocol pipe — captured once, because `redirect_stdout` below mutates
# the global `stdout` and `send_control` must keep writing frames here.
const REAL_STDOUT = stdout

const DEFAULT_PACKAGES = ["Arrow", "DataFrames", "CSV", "Plots"]

mutable struct KernelState
    sessions::Dict{String,Module}
    datasets::Dict{String,Any}
    last_ref::Union{Nothing,String}
    executing::Bool
    interrupted::Bool
    arrows::Vector{Vector{UInt8}}
    arrow_available::Bool
end

KernelState() = begin
    # `import Arrow` binds `Arrow` into Main — `Base.require` only loads the
    # module without a binding, which crashes `Main.Arrow.read(...)` later.
    arrow_available = try
        Main.eval(:(import Arrow))
        try
            Main.eval(:(import DataFrames))
        catch
            # Không có DataFrames.jl: vẫn decode được qua Arrow.Table.
        end
        true
    catch
        false
    end
    KernelState(Dict{String,Module}(), Dict{String,Any}(), nothing,
                false, false, Vector{Vector{UInt8}}(), arrow_available)
end

st = KernelState()
requests = Channel{Any}(Inf)

# ── protobuf wire codec (đủ cho các message của opsense.proto) ────────────

function write_varint(io::IOBuffer, v::UInt64)
    while true
        b = v & 0x7f
        v >>= 0x07
        if v == 0
            write(io, UInt8(b))
            break
        end
        write(io, UInt8(b | 0x80))
    end
end

function read_varint(data::Vector{UInt8}, pos::Int)
    result = UInt64(0)
    shift = 0
    while true
        pos > length(data) && throw(ArgumentError("truncated varint"))
        b = data[pos]
        pos += 1
        result |= UInt64(b & 0x7f) << shift
        (b & 0x80) == 0 && return result, pos
        shift += 7
    end
end

tagvarint(field::Int) = UInt64((field << 3) | 0x00)
tagfixed64(field::Int) = UInt64((field << 3) | 0x01)
taglen(field::Int) = UInt64((field << 3) | 0x02)

pb_varint(io::IOBuffer, field::Int, v::UInt64) =
    (write_varint(io, tagvarint(field)); write_varint(io, v))
pb_bool(io::IOBuffer, field::Int, v::Bool) = pb_varint(io, field, v ? UInt64(1) : UInt64(0))
pb_str(io::IOBuffer, field::Int, s::AbstractString) = begin
    write_varint(io, taglen(field))
    data = codeunits(s)
    write_varint(io, UInt64(sizeof(data)))
    write(io, data)
end
pb_bytes(io::IOBuffer, field::Int, b::Vector{UInt8}) = begin
    write_varint(io, taglen(field))
    write_varint(io, UInt64(length(b)))
    write(io, b)
end
pb_msg(io::IOBuffer, field::Int, payload::Vector{UInt8}) = pb_bytes(io, field, payload)
pb_double(io::IOBuffer, field::Int, x::Float64) =
    (write_varint(io, tagfixed64(field)); write(io, reinterpret(UInt64, x)))

"Decode one protobuf wire-format payload into `field => [(wiretype, value), …]`."
function pb_fields(payload::Vector{UInt8})
    fields = Dict{Int,Vector{Tuple{UInt8,Any}}}()
    pos = 1
    n = length(payload)
    while pos <= n
        key, pos = read_varint(payload, pos)
        field = Int(key >> 3)
        wt = key & 0x07
        if wt == 0x00
            v, pos = read_varint(payload, pos)
            push!(get!(() -> Tuple{UInt8,Any}[], fields, field), (UInt8(0x00), v))
        elseif wt == 0x01
            stop = pos + 7
            stop > n && throw(ArgumentError("truncated fixed64"))
            raw = reinterpret(UInt64, payload[pos:stop])[1]
            v = reinterpret(Float64, raw)
            pos += 8
            push!(get!(() -> Tuple{UInt8,Any}[], fields, field), (UInt8(0x01), v))
        elseif wt == 0x02
            len, pos = read_varint(payload, pos)
            stop = pos + Int(len) - 1
            stop > n && throw(ArgumentError("field $field overruns payload"))
            v = payload[pos:stop]
            pos = stop + 1
            push!(get!(() -> Tuple{UInt8,Any}[], fields, field), (UInt8(0x02), v))
        elseif wt == 0x05
            pos + 3 > n && throw(ArgumentError("truncated fixed32"))
            pos += 4 # fixed32 — không dùng, bỏ qua an toàn
        else
            throw(ArgumentError("unsupported wire type $wt on field $field"))
        end
    end
    return fields
end

first_varint(f::Dict, field::Int, default::UInt64 = UInt64(0)) =
    haskey(f, field) ? f[field][1][2] : default
first_bool(f::Dict, field::Int) = first_varint(f, field, UInt64(0)) != 0
function first_str(f::Dict, field::Int)
    entry = get(f, field, nothing)
    (entry === nothing || entry[1][1] != 0x02) && return ""
    String(copy(entry[1][2]))
end
function all_strs(f::Dict, field::Int)
    out = String[]
    for (wt, v) in get(f, field, Tuple{UInt8,Any}[])
        wt == 0x02 && push!(out, String(copy(v)))
    end
    out
end
first_msg(f::Dict, field::Int) =
    haskey(f, field) ? Vector{UInt8}(copy(f[field][1][2])) : UInt8[]

# ── message builders (những message kernel gửi đi) ─────────────────────────

msg_welcome() = begin
    io = IOBuffer()
    pb_varint(io, 1, UInt64(PROTO_VERSION))
    pb_str(io, 2, KERNEL_NAME)
    pb_str(io, 3, string(VERSION))
    take!(io)
end

msg_package(name::String) = begin
    io = IOBuffer()
    pb_str(io, 1, name)
    available = try
        Base.identify_package(name) !== nothing
    catch
        false
    end
    pb_bool(io, 2, available)
    take!(io)
end

msg_health(packages::Vector{Vector{UInt8}}) = begin
    io = IOBuffer()
    pb_bool(io, 1, true)
    pb_str(io, 2, KERNEL_NAME)
    pb_str(io, 3, string(VERSION))
    for p in packages
        pb_msg(io, 4, p)
    end
    pb_str(io, 5, st.arrow_available ? "ready" : "ready (Arrow.jl missing: no datasets)")
    take!(io)
end

msg_session_handle(session_id::String) = begin
    io = IOBuffer(); pb_str(io, 1, session_id); take!(io)
end

msg_ack(ok::Bool, error::AbstractString = "") = begin
    io = IOBuffer()
    pb_bool(io, 1, ok)
    pb_str(io, 2, String(error))
    take!(io)
end

msg_error_event(kind::String, message::String) = begin
    io = IOBuffer(); pb_str(io, 1, kind); pb_str(io, 2, message); take!(io)
end

msg_value_text(text::String) = begin
    io = IOBuffer(); pb_str(io, 2, text); take!(io)
end

msg_value_dataframe(arrow_ipc::Vector{UInt8}, rows::Int, cols::Int,
                    columns::Vector{String}) = begin
    io = IOBuffer()
    pb_bytes(io, 1, arrow_ipc)
    pb_varint(io, 2, UInt64(rows))
    pb_varint(io, 3, UInt64(cols))
    for c in columns
        pb_str(io, 4, c)
    end
    take!(io)
end

msg_exec_event(request_id::String;
               stdout_line::Union{Nothing,String} = nothing,
               error::Union{Nothing,Vector{UInt8}} = nothing,
               done::Bool = false,
               result_value::Union{Nothing,Vector{UInt8}} = nothing) = begin
    io = IOBuffer()
    pb_str(io, 1, request_id)
    # Oneof: CHỈ ghi đúng một field — tránh decoder nhầm lẫn thứ tự.
    if error !== nothing
        pb_msg(io, 6, error)
    elseif result_value !== nothing
        pb_msg(io, 8, result_value)
    elseif stdout_line !== nothing
        pb_str(io, 2, stdout_line)
    else
        pb_bool(io, 7, true)  # done = true
    end
    take!(io)
end

msg_dataset_ack(dataset_ref::String, rows::Int, ok::Bool, err::AbstractString = "") = begin
    io = IOBuffer()
    pb_str(io, 1, dataset_ref)
    pb_varint(io, 2, UInt64(max(rows, 0)))
    pb_bool(io, 3, ok)
    pb_str(io, 4, String(err))
    take!(io)
end

# ── framing ────────────────────────────────────────────────────────────────

_u32be(len::Integer) =
    UInt8[(len >> 24) & 0xff, (len >> 16) & 0xff, (len >> 8) & 0xff, len & 0xff]

"Wrap a message into an Envelope oneof (`field` theo opsense.proto) rồi gửi."
function send_control(field::Int, payload::Vector{UInt8})
    io = IOBuffer()
    pb_msg(io, field, payload)
    body = take!(io)
    write(REAL_STDOUT, TAG_CONTROL)
    write(REAL_STDOUT, _u32be(length(body)))
    write(REAL_STDOUT, body)
    flush(REAL_STDOUT)
end

read_exact_bytes(stream::IO, n::Int) = read!(stream, Vector{UInt8}(undef, n))

# ── exec ───────────────────────────────────────────────────────────────────

function sleep_interruptible(seconds::Float64)::Bool
    deadline = time() + seconds
    while time() < deadline
        st.interrupted && return true
        sleep(clamp(deadline - time(), 0.001, 0.05))
    end
    return false
end

nrow_compat(t) = try
    Int(size(t, 1))
catch
    0
end

# Arrow.jl hiện đại không định nghĩa `DataFrame`: decode qua `Arrow.Table`,
# và nếu DataFrames.jl có sẵn thì wrap thành DataFrame để vcat/nrow hoạt động.
function table_from_arrow_bytes(seg::Vector{UInt8})
    t = Main.Arrow.Table(seg)
    isdefined(Main, :DataFrames) ? Main.DataFrames.DataFrame(t) : t
end

"Capture `result` về host: bảng (Arrow.jl có sẵn) → DataFrame value, còn lại → repr text."
function classify_result(value)::Vector{UInt8}
    if st.arrow_available && !isa(value, AbstractString) && !isa(value, Number)
        try
            table = if (isdefined(Main, :DataFrames) && value isa Main.DataFrames.DataFrame) ||
                       value isa Main.Arrow.Table
                value
            elseif isdefined(Main, :DataFrames)
                Main.DataFrames.DataFrame(value)
            else
                value
            end
            bytes = collect(Main.Arrow.write(table))
            cols = [string(c) for c in propertynames(table)]
            return msg_value_dataframe(bytes, nrow_compat(table),
                                       length(cols), cols)
        catch
            # fall through to repr
        end
    end
    msg_value_text(repr(value))
end

function emit(request_id::String; stdout_line::Union{Nothing,String} = nothing,
              error::Union{Nothing,Vector{UInt8}} = nothing,
              result_value::Union{Nothing,Vector{UInt8}} = nothing,
              done::Bool = false)
    send_control(9, msg_exec_event(request_id; stdout_line = stdout_line,
                                   error = error, result_value = result_value,
                                   done = done))
end

function exec_code(request_id::String, session::Module, code::String,
                   input_names::Vector{String})
    trimmed = strip(code)
    events = Vector{Tuple{Symbol,Any}}()
    interrupted = false
    err = nothing
    result_payload = nothing

    st.executing = true
    try
        if startswith(trimmed, "sleep:")
            ms = parse(Float64, strip(trimmed[7:end]))
            interrupted = sleep_interruptible(ms / 1000.0)
        elseif startswith(trimmed, "print:")
            push!(events, (:stdout_line, String(strip(trimmed[7:end]))))
        elseif startswith(trimmed, "err:")
            rest = strip(trimmed[5:end])
            sep = findfirst(':', rest)
            kind = isnothing(sep) ? rest : rest[1:prevind(rest, sep)]
            message = isnothing(sep) ? "" : rest[nextind(rest, sep):end]
            push!(events, (:error, msg_error_event(String(kind), String(message))))
        elseif trimmed == "df"
            isnothing(st.last_ref) &&
                throw(ErrorException("no dataset received yet - send one first"))
            table = st.datasets[st.last_ref]
            bytes = collect(Main.Arrow.write(table))
            cols = [string(c) for c in propertynames(table)]
            result_payload = msg_value_dataframe(bytes, nrow_compat(table),
                                                 length(cols), cols)
        else
            # Eval in Main scope — full access to Base and all loaded packages.
            # Redirect stdout to a Pipe so user `print`/`println` becomes
            # `stdout_line` events instead of leaking raw bytes onto the framed
            # pipe (which would desync the host's frame decoder). The Pipe is
            # drained on a background thread (mirrors the stdin `reader_loop`)
            # so output larger than the OS pipe buffer cannot deadlock.
            rd, wr = redirect_stdout()
            reader = Threads.@spawn String(read(rd))
            try
                # Bind pushed datasets into the session scope: "@1" → `_df_1`
                # (cùng quy ước với kernel Python).
                for name in input_names
                    haskey(st.datasets, name) || continue
                    var = Symbol(replace(name, "@" => "_df_"))
                    Core.eval(session, Expr(:(=), var, st.datasets[name]))
                end
                result = Core.eval(session, Meta.parseall(code))
                if !isnothing(result)
                    result_payload = msg_value_text(repr(result))
                end
            finally
                # Close the write end so the reader sees EOF, then restore the
                # real protocol pipe before any frame is written.
                close(wr)
                redirect_stdout(REAL_STDOUT)
            end
            captured = try
                fetch(reader)
            catch
                ""
            end
            for line in split(captured, '\n')
                if !isempty(strip(line))
                    push!(events, (:stdout_line, String(line)))
                end
            end
        end
    catch exc
        err = "$(typeof(exc)): $(sprint(showerror, exc))"
    finally
        st.executing = false
    end

    if interrupted
        push!(events, (:error, msg_error_event("cancelled", "interrupted by host")))
    elseif err !== nothing
        push!(events, (:error, msg_error_event("julia_exception", err)))
    end
    result_payload !== nothing &&
        push!(events, (:result_value, result_payload))
    push!(events, (:done, nothing))

    for (what, payload) in events
        if what === :stdout_line
            emit(request_id; stdout_line = payload::String)
        elseif what === :error
            emit(request_id; error = payload::Vector{UInt8})
        elseif what === :result_value
            emit(request_id; result_value = payload::Vector{UInt8})
        else
            emit(request_id; done = true)
        end
    end
end

# ── dispatch ───────────────────────────────────────────────────────────────

"Handle one Envelope payload. Trả về false khi kernel nên thoát."
function handle(envelope::Vector{UInt8})::Bool
    top = pb_fields(envelope)
    field = first(sort(collect(keys(top))))
    if field == 1 # hello
        hello = pb_fields(first_msg(top, 1))
        if first_varint(hello, 1) != PROTO_VERSION
            @warn "protocol mismatch" host_version = first_varint(hello, 1)
            return false
        end
        send_control(2, msg_welcome())
    elseif field == 12 # health_request
        packages = [msg_package(n) for n in DEFAULT_PACKAGES]
        send_control(13, msg_health(packages))
    elseif field == 3 # start_session
        params = pb_fields(first_msg(top, 3))
        sid = first_str(params, 1)
        st.sessions[sid] = Main  # eval trực tiếp trong Main — full access Base
        send_control(4, msg_session_handle(sid))
    elseif field == 5 # close_request
        delete!(st.sessions, first_str(pb_fields(first_msg(top, 5)), 1))
        send_control(7, msg_ack(true))
    elseif field == 10 # dataset_header — ARROW frames đã vào buffer trước đó
        header = pb_fields(first_msg(top, 10))
        sid = first_str(header, 1)
        ref = first_str(header, 2)
        if !haskey(st.sessions, sid)
            send_control(11, msg_dataset_ack(ref, 0, false, "unknown session $sid"))
            return true
        end
        if !st.arrow_available
            send_control(11, msg_dataset_ack(ref, 0, false,
                "Arrow.jl not installed in this Julia depot"))
            return true
        end
        tables = [table_from_arrow_bytes(seg) for seg in copy(st.arrows)]
        empty!(st.arrows)
        table = length(tables) == 1 ? tables[1] : reduce(vcat, tables)
        rows = nrow_compat(table)
        st.datasets[ref] = table
        st.last_ref = ref
        send_control(11, msg_dataset_ack(ref, rows, true))
    elseif field == 8 # code_request
        req = pb_fields(first_msg(top, 8))
        rid = first_str(req, 1)
        sid = first_str(req, 2)
        haskey(st.sessions, sid) || begin
            emit(rid; error = msg_error_event("bad_request", "unknown session $sid"),
                 done = true)
            return true
        end
        exec_code(rid, st.sessions[sid], first_str(req, 3), all_strs(req, 4))
    elseif field == 6 # interrupt_request
        if st.executing
            st.interrupted = true # cooperative — checked trong sleep loops
        else
            send_control(7, msg_ack(true))
        end
    elseif field == 15 # shutdown
        send_control(7, msg_ack(true))
        return false
    else
        send_control(7, msg_ack(false, "unexpected envelope field: $field"))
    end
    return true
end

# ── main loop ──────────────────────────────────────────────────────────────

function reader_loop()
    try
        while true
            header = read_exact_bytes(stdin, 5)
            frame_len = UInt64(header[2]) << 24 | UInt64(header[3]) << 16 |
                        UInt64(header[4]) << 8 | UInt64(header[5])
            payload = read_exact_bytes(stdin, Int(frame_len))
            if header[1] == TAG_CONTROL
                put!(requests, ("control", payload))
            elseif header[1] == TAG_ARROW
                push!(st.arrows, payload)
            end
        end
    catch
        put!(requests, ("eof", nothing))
    end
end

function main()::Int32
    errormonitor(Threads.@spawn reader_loop())
    while true
        kind, payload = take!(requests)
        if kind == "eof"
            return 0
        end
        kind == "control" && handle(payload) || return 0
    end
end

exit(main())
