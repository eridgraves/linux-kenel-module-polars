use polars::prelude::*;
use std::io::Write;
use std::net::TcpStream;
use anyhow::Result;

fn main() -> Result<()> {

    let df = create_sample_dataframe()?;
    
    // Convert DataFrame to (JSON) bytes
    let json_data = df_to_json(&df)?;
    
    send_dataframe_via_socket(&json_data, "127.0.0.1:3030")?;
    
    println!("DataFrame sent successfully!");
    Ok(())
}

fn create_sample_dataframe() -> PolarsResult<DataFrame> {

    df! {
        "id" => [1, 2, 3, 4, 5],
        "name" => ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value" => [10.5, 20.3, 15.8, 30.2, 25.1],
    }
}

// Convert DataFrame to JSON string
fn df_to_json(df: &DataFrame) -> Result<String> {
    
    let mut buffer = Vec::new();
    let mut df_clone = df.clone(); 
    
    JsonWriter::new(&mut buffer)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df_clone)?; // make clone to convert data
    
    Ok(String::from_utf8(buffer)?)
}

fn send_dataframe_via_socket(data: &str, address: &str) -> Result<()> {

    let mut stream = TcpStream::connect(address)?;

    let data_len = data.len() as u32;
    stream.write_all(&data_len.to_be_bytes())?; // Send length: 4 Bytes
    
    // Send actual data
    stream.write_all(data.as_bytes())?;
    stream.flush()?;
    
    println!("Sent {} bytes to {}", data.len(), address);
    Ok(())
}