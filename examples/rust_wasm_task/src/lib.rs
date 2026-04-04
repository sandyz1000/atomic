use std::alloc::{Layout, alloc, dealloc};
use std::slice;

fn pack_result(ptr: *mut u8, len: usize) -> i64 {
    ((ptr as u64) << 32 | len as u64) as i64
}

#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: i32) -> i32 {
    let layout = Layout::from_size_align(len.max(1) as usize, 1).unwrap();
    unsafe { alloc(layout) as i32 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dealloc(ptr: i32, len: i32) {
    if ptr == 0 || len <= 0 {
        return;
    }

    let layout = Layout::from_size_align(len as usize, 1).unwrap();
    unsafe { dealloc(ptr as *mut u8, layout) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_map(ptr: i32, len: i32) -> i64 {
    let input = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    let mut output = input
        .iter()
        .map(|byte| byte.wrapping_add(1))
        .collect::<Vec<u8>>();
    let packed = pack_result(output.as_mut_ptr(), output.len());
    std::mem::forget(output);
    packed
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn run_reduce(ptr: i32, len: i32) -> i64 {
    let input = unsafe { slice::from_raw_parts(ptr as *const u8, len as usize) };
    let sum = input.iter().fold(0_u32, |acc, byte| acc + u32::from(*byte));
    let mut output = sum.to_le_bytes().to_vec();
    let packed = pack_result(output.as_mut_ptr(), output.len());
    std::mem::forget(output);
    packed
}
