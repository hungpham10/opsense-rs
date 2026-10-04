/// disk_capacity_forecast.rhai
/// Dự đoán chỉ số có trần: **hướng đi** + **biên đường chéo**.
///
/// `capacity_forecast` không chuyên biệt cho đĩa — nó dùng được cho bất kì chỉ
/// số nào có một `capacity` và bò dần về phía nó (volume, hạn mức API,
/// `MemTotal`, pin…). Đĩa chỉ là ví dụ ở đây, với trần cứng là 100%.
///
/// Mỗi đĩa cho ra 3 observation:
///
///   disk_capacity_hours:<mp>  — còn bao lâu nữa mép trên chạm 100%. Đây là
///                                con số để cảnh báo, vì nó tính cả dao động.
///                                `value = -1` khi disk không đi lên (không có
///                                mốc chạm trong tương lai).
///   disk_capacity_trend:<mp>  — bao nhiêu giờ nữa **đường xu hướng** (không tính
///                                dao động) chạm 100%. Lớc dưới con số cảnh báo,
///                                nên lập kế hoạch theo cái này.
///   disk_capacity_band:<mp>   — value = biên độ tính bằng **ô lưới**.
///                                `0.4` = mép trên/dưới lệch nhau chưa tới một
///                                dải: disk đang đi trong một dải, nên số giờ ở
///                                trên là dự đoán trên dữ liệu nhiễu.
///
/// Cấu hình qua params của node:
///
///   [pipeline.components.params]
///   cells            = 12     # max_bit của grid (2^cells ô tối đa)
///   recent_secs      = 86400  # cửa sổ quan sát (1 ngày)
///   min_samples      = 12     # dưới ngưỡng thì bỏ qua đĩa đó
///
/// LƯU Ý HIỆN NĂNG: window đầu tiên của node (cursor=0) chứa cả lịch sử trong
/// persistence. Script cắt cửa sổ ở pass 2 (rẻ, chỉ so số) rồi mới group và
/// fit — group trực tiếp trên hàng chục nghìn điểm sẽ vượt `max_map_size` của
/// Rhai.

/// Mountpoint của một observation; thiếu label thì gộp về "?".
/// Tách ra để một chỗ duy nhất quyết định tên đĩa — script dựng label và
/// so khớp đều phải dùng cùng một quy tắc.
fn mountpoint_of(x) {
    let mp = x["labels"]["mountpoint"];
    if mp == () {
        return "?";
    }
    mp
}

fn process(observations) {
    let cells = 12;
    try { cells = param_cells } catch { 12 };
    let recent_secs = 86400;
    try { recent_secs = param_recent_secs } catch { 86400 };
    let min_samples = 12;
    try { min_samples = param_min_samples } catch { 12 };

    if observations.len() == 0 {
        return [];
    }

    // Pass 1 (rẻ): mốc thời gian lớn nhất của cửa sổ
    let tmax = 0;
    for x in observations {
        let t = x["ts"];
        if t > tmax {
            tmax = t;
        }
    }

    // Pass 2 (rẻ): chỉ giữ dữ liệu gần nhất
    let recent = [];
    let cutoff = tmax - recent_secs;
    for x in observations {
        if x["ts"] > cutoff {
            recent.push(x);
        }
    }
    if recent.len() == 0 {
        return [];
    }

    // Danh sách đĩa trong cửa sổ. Dùng Map làm **set** (`= true`) thay vì gom
    // luôn chuỗi, để không phải gọi `push` qua giá trị của Map (`Dynamic`) —
    // thao tác đó phụ thuộc dispatch runtime.
    let disks = #{};
    for x in recent {
        disks[mountpoint_of(x)] = true;
    }

    let out = [];
    for mp in disks.keys() {
        // Gom điểm của riêng đĩa này. `pts` khởi tạo bằng `[]` nên Rhai biết
        // kiểu là `Array` và `push` là gọi tĩnh — an toàn hơn hẳn.
        let pts = [];
        for x in recent {
            if mountpoint_of(x) == mp {
                pts.push(#{ ts: x["ts"], value: x["value"] });
            }
        }
        if pts.len() < min_samples {
            continue;
        }

        // `100.0` vì input là **phần trăm** đã dùng. Đổi sang byte thì truyền
        // tổng dung lượng thay vì hằng số này.
        let f = capacity_forecast(pts, 100.0, 0, cells, min_samples);
        if type_of(f) == "() {
            continue;   // không đủ dữ liệu để nói bất cứ điều gì
        }

        let tr = capacity_trend(f);
        let dir = capacity_direction(f);
        let osc = capacity_oscillating(f);
        let slope_day = trend_slope_per_day(tr);
        let r2 = trend_r2(tr);
        let drift = capacity_drift(f);
        let band = capacity_envelope_cells(f);
        let headroom = capacity_headroom_rel(f);
        let cell_now = capacity_current_cell(f);
        let cell_top = capacity_top_cell(f);

        // `()` = disk không đi lên, nên không có mốc chạm trần. Ép -1 để
        // observation vẫn tồn tại (thiếu series thì alert không bắt được).
        let h_full = capacity_hours_to_full(f);
        let h_trend = capacity_hours_to_trend_full(f);
        let h_top = capacity_hours_to_top_cell(f);
        let hours_full = if type_of(h_full) == "()" { -1.0 } else { h_full };
        let hours_trend = if type_of(h_trend) == "()" { -1.0 } else { h_trend };
        let hours_top = if type_of(h_top) == "()" { -1.0 } else { h_top };

        // 1. Mép trên chạm đầy — con số để cảnh báo
        out.push(#{
            ts: now_secs(),
            metric_id: "disk_capacity_hours:" + mp,
            kind: "metric",
            signal: "raw",
            value: hours_full,
            labels: #{
                mountpoint: mp,
                direction: dir,
                oscillating: if osc { "yes" } else { "no" },
                hours_trend: hours_trend.to_string(),
                hours_top_cell: hours_top.to_string(),
                headroom_rel: headroom.to_string(),
            },
        });

        // 2. Đường xu hướng chạm đầy — con số để lập kế hoạch
        out.push(#{
            ts: now_secs(),
            metric_id: "disk_capacity_trend:" + mp,
            kind: "metric",
            signal: "raw",
            value: hours_trend,
            labels: #{
                mountpoint: mp,
                direction: dir,
                slope_per_day: slope_day.to_string(),
                r2: r2.to_string(),
                drift: drift.to_string(),
                oscillating: if osc { "yes" } else { "no" },
            },
        });

        // 3. Bề rộng biên đường chéo, tính bằng ô lưới
        out.push(#{
            ts: now_secs(),
            metric_id: "disk_capacity_band:" + mp,
            kind: "metric",
            signal: "raw",
            value: band,
            labels: #{
                mountpoint: mp,
                direction: dir,
                oscillating: if osc { "yes" } else { "no" },
                cell: cell_now.to_string(),
                top_cell: cell_top.to_string(),
                amplitude: capacity_amplitude(f).to_string(),
            },
        });
    }

    out
}
