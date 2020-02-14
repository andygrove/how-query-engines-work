
pub trait DataFrame {

    fn select(&self, expr: Vec<Expr>) -> Box<dyn DataFrame>;

    fn filter(&self, expr: Expr) -> Box<dyn DataFrame>;

    fn parquet(&self, path: &str) -> Box<dyn DataFrame>;

    fn collect(&self) -> Box<dyn Iterator<Item = Box<dyn RecordBatch>>>;

}

pub trait RecordBatch {
    //TODO
}

pub enum Expr {
    ColumnIndex(usize),
    //TODO add others
}