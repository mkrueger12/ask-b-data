use reqwest::{self, Error, Response};
use scraper::{Html, Selector};


pub fn do_throttled_request(url: &str) -> Result<String, Error> {
    // See the real code for the throttling - it's omitted here for clarity
    let response = reqwest::blocking::get(url)?;
    response.text()
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


async fn process_station(record: TableData) -> Result<reqwest::Response, reqwest::Error> {
    let site_name = &record.site_name;
    let state = &record.state;
    let station_id: String = site_name.chars()
        .filter(|c| c.is_digit(10))
        .collect();

    let url: &str = format!(r#"https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/start_of_period/{station_id}:{state}:SNTL%7Cid=""|name/-1,0/name,stationId,state.code,network.code,elevation,latitude,longitude,county.name,WTEQ::value,WTEQ::pctOfMedian_1991,SNWD::value,TMAX::value,TMIN::value,TOBS::value,SNDN::value?fitToScreen=false"#,
        station_id = station_id,
        state = state
    ).as_str();
        
    
    let response: = reqwest::get(url).await?;

    response

}


#[tokio::main]
async fn main() {
    let http_data: Result<String, reqwest::Error>  = do_throttled_request("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist");
    let table = create_struct_from_table(&http_data.unwrap());
    let data: = process_station(table[4].clone()).await?;
    println!("{:#?}", data);
}
