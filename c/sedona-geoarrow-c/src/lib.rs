use std::ffi::CStr;

mod error;
mod geoarrow_c;
mod geoarrow_c_bindgen;
pub mod kernels;

pub fn geoarrow_c_version() -> String {
    let char_ptr = unsafe { geoarrow_c_bindgen::SedonaDBGeoArrowVersion() };
    let c_str = unsafe { CStr::from_ptr(char_ptr).to_str().unwrap() };
    c_str.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert_eq!(geoarrow_c_version(), "0.2.0-SNAPSHOT")
    }
}
