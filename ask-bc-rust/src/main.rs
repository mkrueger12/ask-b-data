use reqwest::{self, Error, Response};
use scraper::{Html, Selector};
use polars::prelude::*;
use std::io::Cursor;

async fn request(url: &str) -> Result<String, Error> {
    // See the real code for the throttling - it's omitted here for clarity
    let response: Result<reqwest::Response, reqwest::Error> = Ok(reqwest::get(url).await?);
    response.unwrap().text().await
}

#[derive(Debug, Clone)]
struct TableData {
    // Define the fields of the struct based on the table columns
    ntwk: String,
    state: String,
    site_name: String,
    ts: String,
    start: String,
    lat: String,
    lon: String,
    elev: String,
    county: String,
    huc: String,
}

fn create_struct_from_table(html: &str) -> Vec<TableData> {
    let fragment = Html::parse_fragment(html);
    let table_selector = Selector::parse("table").unwrap();
    let row_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("td").unwrap();

    let table = fragment.select(&table_selector).skip(1).next().unwrap();
    let rows = table.select(&row_selector);

    let mut data = Vec::new();

    for row in rows {
        let cells = row.select(&cell_selector);
        let mut table_data = TableData {
            ntwk: String::new(),
            state: String::new(),
            site_name: String::new(),
            ts: String::new(),
            start: String::new(),
            lat: String::new(),
            lon: String::new(),
            elev: String::new(),
            county: String::new(),
            huc: String::new(),
        };

        for (i, cell) in cells.enumerate() {
            match i {
                0 => table_data.ntwk = cell.text().collect(),
                1 => table_data.state = cell.text().collect(),
                2 => table_data.site_name = cell.text().collect(),
                3 => table_data.ts = cell.text().collect(),
                4 => table_data.start = cell.text().collect(),
                5 => table_data.lat = cell.text().collect(),
                6 => table_data.lon = cell.text().collect(),
                7 => table_data.elev = cell.text().collect(),
                8 => table_data.county = cell.text().collect(),
                9 => table_data.huc = cell.text().collect(),                
                _ => (),
            }
        }

        data.push(table_data);
    }

    data
}


async fn process_station(record: TableData) -> Result<String, Error> {
    let site_name = &record.site_name;
    let state = &record.state;
    let station_id: String = site_name.chars()
        .filter(|c| c.is_digit(10))
        .collect();

    let url: String = format!(r#"https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/start_of_period/{station_id}:{state}:SNTL%7Cid=""|name/-1,0/name,stationId,state.code,network.code,elevation,latitude,longitude,county.name,WTEQ::value,WTEQ::pctOfMedian_1991,SNWD::value,TMAX::value,TMIN::value,TOBS::value,SNDN::value?fitToScreen=false"#,
        station_id = station_id,
        state = state
    );
    
    let response: Response = reqwest::get(url).await?;
    println!("Status: {}", response.status());

    let body: String = response.text().await?;
    //println!("Body:\n{}", body);

    // Convert the body string to a byte slice
    let body_bytes = body.as_bytes();

    let mut df = CsvReader::new(Cursor::new(body_bytes))
    .has_header(true)
    .with_comment_char(Some(b'#'))
    .finish()
    .unwrap();

        // Set column names
    let column_mapping = [
            ("Date", "date"),
            ("Station Name", "station_name"),
            ("Station Id", "station_id"),
            ("State Code", "state_code"),
            ("Network Code", "network_code"),
            ("Elevation (ft)", "elevation_ft"),
            ("Latitude", "latitude"),
            ("Longitude", "longitude"),
            ("County Name", "county_name"),
            ("Snow Water Equivalent (in) Start of Day Values", "snow_water_equivalent_in"),
            ("Snow Water Equivalent % of Median (1991-2020)", "snow_water_equivalent_median_percentage"),
            ("Snow Depth (in) Start of Day Values", "snow_depth_in"),
            ("Air Temperature Maximum (degF)", "max_temp_degF"),
            ("Air Temperature Minimum (degF)", "min_temp_degF"),
            ("Air Temperature Observed (degF) Start of Day Values", "observed_temp_degF"),
            ("Snow Density (pct) Start of Day Values", "snow_density_percentage"),
        ];

    for (old_name, new_name) in &column_mapping {
            let df: &mut DataFrame = df.rename(old_name, new_name).unwrap(); // What is going on here.
        }
    
    let new_df = df;

    // Display the DataFrame
    println!("{:?}", new_df);
    
    Ok(new_df.to_string()) // update this after all functionaity is working

}


#[tokio::main]
async fn main() {
    let http_data: Result<String, Error>  = request("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist").await;

    //println!("body = {:?}", &http_data.unwrap());
    let table = create_struct_from_table(&http_data.unwrap());
    let data: Result<String, Error> = process_station(table[4].clone()).await; // sending the 4th record for testing
    println!("{:#?}", data);
    
}
