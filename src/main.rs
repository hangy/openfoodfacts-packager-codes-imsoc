use futures::StreamExt;
use geocoding::{Forward, Openstreetmap, Point};
use serde::*;
use std::{collections::HashMap, io, thread, time};
use tokio::*;
use urlencoding::encode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let countries_categories = fetch_valid_categories_by_countries().await?;
    let establishments_by_country = map_establishments_to_countries(countries_categories).await?;
    let packager_codes = geocode_all_countries(establishments_by_country).await?;
    write_packager_codes_csv(packager_codes)?;
    return Ok(());
}

#[derive(Serialize, Debug)]
struct PackagerCode {
    name: String,
    code: String,
    lat: f64,
    lng: f64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Establishment {
    operator_id: i32,
    operator_name: Option<String>,
    address: Address,
    approval_number: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Address {
    street: Street,
    city_reference: City,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Street {
    value: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct City {
    city_id: i32,
    postal_code: Option<String>,
    name: Option<String>,
    country: Country,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CountryStatus {
    id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Country {
    code: String,
    status: CountryStatus,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ClassificationSectionId {
    id: String,
    code: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CountryCategory {
    sequence_number: i32,
    country: Country,
    classification_section_id: ClassificationSectionId,
    number_of_establishments: i32,
}

async fn fetch_establishments_for_country_and_section(
    country: String,
    section: String,
) -> Result<Vec<Establishment>, Box<dyn std::error::Error>> {
    let mut offset = 0;
    let page_size = 1;
    let mut establishments = Vec::<Establishment>::default();
    loop {
        let mut establishments_page = fetch_establishments_for_country_and_section_page(
            country.to_owned(),
            section.to_owned(),
            offset,
            page_size,
        )
        .await?;
        if establishments_page.is_empty() {
            break;
        }
        establishments.append(&mut establishments_page);
        offset += page_size;
        break;
    }
    return Ok(establishments);
}

async fn fetch_establishments_for_country_and_section_page(
    country: String,
    section: String,
    offset: i32,
    max: i32,
) -> Result<Vec<Establishment>, Box<dyn std::error::Error>> {
    let offset_param: String = offset.to_string();
    let max_param: String = max.to_string();

    let base_url = format!(
        "https://webgate.ec.europa.eu/tracesnt/directory/publication/establishment/establishments/{cc}/{section}?sort=operatorName",
        cc = encode(&country),
        section = encode(&section));
    let url =
        url::Url::parse_with_params(&base_url, &[("max", max_param), ("offset", offset_param)])?;

    let a_second = time::Duration::from_millis(1000);
    thread::sleep(a_second);

    let resp: Vec<Establishment> = reqwest::get(url).await?.json().await?;
    return Ok(resp);
}

fn write_packager_codes_csv(
    packager_codes: Vec<PackagerCode>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    for c in packager_codes {
        wtr.serialize(c)?;
    }

    wtr.flush()?;
    return Ok(());
}

async fn geocode_all_countries(
    establishments_by_country: HashMap<String, Vec<Establishment>>,
) -> Result<Vec<PackagerCode>, Box<dyn std::error::Error>> {
    let a_second = time::Duration::from_millis(1000);
    let mut packager_codes: Vec<PackagerCode> = vec![];
    for (_, establishments) in &establishments_by_country {
        for e in establishments {
            if e.approval_number.is_none()
                || e.approval_number.to_owned().is_some_and(|f| f.is_empty())
            {
                continue;
            }

            let mut address_components: Vec<String> = vec![];
            if !e.address.street.value.is_empty() && !e.address.street.value.eq(".") {
                address_components.push(e.address.street.value.clone());
            }

            if !e.address.city_reference.postal_code.is_none() {
                let postal_code = e.address.city_reference.postal_code.clone().unwrap();
                if !postal_code.is_empty() {
                    address_components.push(postal_code);
                }
            }

            if !e.address.city_reference.country.code.is_empty() {
                address_components.push(e.address.city_reference.country.code.clone());
            }

            let address = address_components.join(", ");
            let res = task::spawn_blocking(move || {
                thread::sleep(a_second);
                let osm = Openstreetmap::new();
                let r: Vec<Point<f64>> = osm.forward(&address).unwrap_or_default();
                return r;
            })
            .await?;
            let empty = Point::<f64>::new(0f64, 0f64);
            let f = res.first().unwrap_or(&empty);
            if f.x() <= 0f64 || f.y() <= 0f64 {
                continue;
            }

            packager_codes.push(PackagerCode {
                code: format!(
                    "{} {} EC",
                    e.address.city_reference.country.code.clone(),
                    e.approval_number.clone().unwrap()
                ),
                name: e.operator_name.clone().unwrap_or_default(),
                lat: f.x(),
                lng: f.y(),
            });
        }
    }

    return Ok(packager_codes);
}

async fn map_establishments_to_countries(
    countries_categories: Vec<CountryCategory>,
) -> Result<HashMap<String, Vec<Establishment>>, Box<dyn std::error::Error>> {
    let mut grouped_map: HashMap<String, Vec<Establishment>> = HashMap::new();
    for c in countries_categories {
        let data = fetch_establishments_for_country_and_section(
            c.country.code.to_owned(),
            c.classification_section_id.code.to_owned(),
        )
        .await?;

        let mut stream = futures::stream::iter(data);
        while let Some(item) = stream.next().await {
            let key = item.address.city_reference.country.code.clone();
            grouped_map.entry(key).or_insert_with(Vec::new).push(item);
        }
    }

    return Ok(grouped_map);
}

async fn fetch_valid_categories_by_countries(
) -> Result<Vec<CountryCategory>, Box<dyn std::error::Error>> {
    let country_categories = fetch_categories_by_countries().await?;
    let filter = futures::stream::iter(country_categories).filter(|current| {
        let country_is_valid = current.country.status.id == "V";
        let section_is_not_empty = current.number_of_establishments > 0;
        let result = country_is_valid && section_is_not_empty;
        return std::future::ready(result);
    });

    let filtered = filter.collect::<Vec<_>>().await;
    return Ok(filtered);
}

async fn fetch_categories_by_countries() -> Result<Vec<CountryCategory>, Box<dyn std::error::Error>>
{
    let mut offset = 0;
    let page_size = 5;
    let mut country_categories = Vec::<CountryCategory>::default();

    loop {
        let mut categories_by_countries =
            fetch_categories_by_countries_page(offset, page_size).await?;
        if categories_by_countries.is_empty() {
            break;
        }
        country_categories.append(&mut categories_by_countries);
        offset += page_size;
        break;
    }

    return Ok(country_categories);
}

async fn fetch_categories_by_countries_page(
    offset: i32,
    max: i32,
) -> Result<Vec<CountryCategory>, Box<dyn std::error::Error>> {
    let offset_param: String = offset.to_string();
    let max_param: String = max.to_string();

    let url  = url::Url::parse_with_params("https://webgate.ec.europa.eu/tracesnt/directory/publication/establishment?sort=country.translation",
        &[("max", max_param), ("offset", offset_param)])?;
    let resp: Vec<CountryCategory> = reqwest::get(url).await?.json().await?;
    return Ok(resp);
}
