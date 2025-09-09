use std::cell::Cell;

thread_local! {
    static DRY_RUN: Cell<bool> = const { Cell::new(false) };
}

pub fn is_dry_run() -> bool {
    DRY_RUN.with(|c| c.get())
}

pub fn with_dry_run_enabled<R>(enabled: bool, f: impl FnOnce() -> R) -> R {
    struct Guard(bool);
    impl Drop for Guard {
        fn drop(&mut self) {
            DRY_RUN.with(|c| c.set(self.0));
        }
    }
    let prev = DRY_RUN.with(|c| {
        let p = c.get();
        c.set(enabled);
        p
    });
    let _g = Guard(prev);
    f()
}
