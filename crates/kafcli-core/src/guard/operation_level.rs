/// How much an operation changes the cluster, which decides how strictly it is guarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationLevel {
    Read,
    Write,
    Destructive,
}
