(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))

  (func $alloc (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    global.get $heap
    local.set $ptr
    global.get $heap
    local.get $len
    i32.add
    global.set $heap
    local.get $ptr)

  (func (export "run_map") (param $ptr i32) (param $len i32) (result i64)
    (local $out i32)
    (local $i i32)
    local.get $len
    call $alloc
    local.set $out
    block $done
      loop $copy
        local.get $i
        local.get $len
        i32.ge_u
        br_if $done
        local.get $out
        local.get $i
        i32.add
        local.get $ptr
        local.get $i
        i32.add
        i32.load8_u
        i32.const 1
        i32.add
        i32.store8
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $copy
      end
    end
    local.get $out
    i64.extend_i32_u
    i64.const 32
    i64.shl
    local.get $len
    i64.extend_i32_u
    i64.or)

  (func (export "run_reduce") (param $ptr i32) (param $len i32) (result i64)
    (local $out i32)
    (local $i i32)
    (local $sum i32)
    i32.const 4
    call $alloc
    local.set $out
    block $done
      loop $sum_loop
        local.get $i
        local.get $len
        i32.ge_u
        br_if $done
        local.get $sum
        local.get $ptr
        local.get $i
        i32.add
        i32.load8_u
        i32.add
        local.set $sum
        local.get $i
        i32.const 1
        i32.add
        local.set $i
        br $sum_loop
      end
    end
    local.get $out
    local.get $sum
    i32.store
    local.get $out
    i64.extend_i32_u
    i64.const 32
    i64.shl
    i64.const 4
    i64.or))
