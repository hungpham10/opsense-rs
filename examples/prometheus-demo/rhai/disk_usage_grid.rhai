/// disk_usage_grid.rhai
/// Phân tích % disk usage qua AnalysisGrid (sàng phân cấp tìm band tối ưu).
/// Input: array các observation map {ts, value} với value = % used (0..100)
/// Output: thông tin grid đã fit
///
/// Số band cấu hình được qua `params` của node rhai_transform (mặc định 10):
///
///   [pipeline.components.params]
///   cells = 10

fn process(observations) {
    let cells = 10;
    try { cells = param_cells } catch { 10 };

    // boundary vật lý: 0% → 100% full
    let g = grid_fit(observations, 0.0, 100.0, cells);

    // các band được phát hiện tự động bởi sieve algorithm
    let bands = num_cells(g);
    let step = grid_step(g);

    // đếm crossings (số lần value nhảy qua band boundary)
    let xings = grid_crossings(g, observations);

    // occupancy histogram: bucket 1 giờ × band
    let occ = grid_occupancy(g, observations, 3600);

    // mỗi band: index, low%, high%
    let ranges = grid_ranges(g);

    // trả về kết quả phân tích grid
    #{
        bands:      bands,
        step:       step,
        crossings:  xings,
        ranges:     ranges,
        occ:        occ,
    }
}
