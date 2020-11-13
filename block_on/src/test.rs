#![allow(dead_code)]

use block_on::block_on;
struct Tokio {}
struct AsyncStd {}

#[block_on("tokio")]
impl Tokio {
    async fn test_async(&self) {}

    async fn test_async_static() {}

    async fn test_async_mut(&mut self) {}

    async fn test_async_arg(&self, _i: u8) {}

    async fn test_async_static_arg(_i: u8) {}

    async fn test_async_pointer_arg(&self, _i: &u8) {}

    // Fails with mut in argument
    // async fn test_async_pointer_arg(&self, mut _i: u8) {}

    async fn test_async_static_pointer_arg(_i: &u8) {}

    async fn test_async_args(&self, _i: u8, _j: u8, _k: u8) {}

    async fn test_async_static_args(_i: u8, _j: u8, _k: u8) {}

    async fn test_async_pointer_args(&self, _i: u8, _j: &u8, _k: &mut u8) {}

    async fn test_async_static_pointer_args(_i: u8, _j: &u8, _k: &mut u8) {}
}

fn main() {}
