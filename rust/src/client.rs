use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::protobuf;

use arrow::flight::flight_data_to_batch;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::logicalplan::*;
//use flight::flight_descriptor;
use flight::flight_service_client::FlightServiceClient;
use flight::Ticket;
use prost::Message;

pub async fn execute_query(host: &str, port: usize, plan: LogicalPlan) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {

    //TODO need to avoid connecting per request
    let mut client = FlightServiceClient::connect(format!("http://{}:{}", host, port)).await?;

    let serialized_plan: protobuf::LogicalPlanNode = plan.try_into().unwrap();
    let mut buf: Vec<u8> = Vec::with_capacity(serialized_plan.encoded_len());
    serialized_plan.encode(&mut buf).unwrap();

    let request = tonic::Request::new(Ticket { ticket: buf });

    let mut stream = client.do_get(request).await?.into_inner();

    // the schema should be the first message returned, else client should error
    let flight_data = stream.message().await?.unwrap();

    // convert FlightData to a stream
    let schema = Arc::new(Schema::try_from(&flight_data)?);

    // all the remaining stream messages should be dictionary and record batches
    let mut batches = vec![];
    while let Some(flight_data) = stream.message().await? {
        // the unwrap is infallible and thus safe
        let record_batch = flight_data_to_batch(&flight_data, schema.clone())?.unwrap();
        batches.push(record_batch);
    }

    Ok(batches)
}
