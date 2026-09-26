//! Converters — components đổi **hình dạng** dữ liệu đầu vào.
//!
//! Khác với [`crate::station`] (mỗi node sở hữu một station) và
//! [`crate::http`] (kéo dữ liệu từ ngoài): converter lấy một dạng dữ liệu và
//! phát ra dạng khác, giữ nguyên cấu trúc message.
//!
//! Khác với converter của `opsense_mlib` (`json_2_json`, `websocket_2_json` là
//! payload → payload thuần, không đụng station): converter ở đây có thể **ghi
//! vào station của chính node**. Lý do là chiều phụ thuộc — `opsense-core` phụ
//! thuộc `opsense-mlib`, nên mlib không dùng được `Station` /
//! `TimeseriesStation` / `Observation`. Component cần ghi station thì không thể
//! đặt ở mlib.
//!
//! Component type được re-export ở đây để binary/test chỉ nhắc đường dẫn cũng
//! ép đăng ký typetag vào link — giống `mlib::vector::components::converters`.
//!
//! [`crate::station`]: https://docs.rs/opsense-components

mod tick2candle;

// Re-export để config/test nhắc `opsense_components::converters::Tick2Candle`
// cũng kéo theo đăng ký typetag.
pub use tick2candle::Tick2Candle;
