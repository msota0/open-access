import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import time
import pandas as pd
import json

# Helper function to fetch articles from an API
def fetch_articles_batch(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to fetch articles with status code: {response.status_code}")
        return None

# Function to handle rate limiting
def rate_limit_wait():
    time.sleep(1)  # Adjust this sleep time based on API rate limits

# Function for PubMed API
def call_pubmed_api_call():
    columns = ['title', 'published date', 'authors', 'affiliations', 'keyword', 'meshing text', 'url', 'source']
    pubmed_df = pd.DataFrame(columns=columns)
    
    current_year = datetime.now().year
    start_year = current_year - 5
    end_year = current_year

    search_term = "University of Mississippi[Affiliation]"
    search_url_template = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"
        "db=pubmed&term={search_term}&retmode=xml&mindate={start_date}&maxdate={end_date}&retstart={start}&retmax={max_results}"
    )

    all_uids = []
    start = 0
    max_results = 2000

    while True:
        search_url = search_url_template.format(
            search_term=search_term,
            start_date=f"{start_year}/01/01",
            end_date=f"{end_year}/12/31",
            start=start,
            max_results=max_results
        )
        
        search_response = requests.get(search_url)
        if search_response.status_code != 200:
            print(f"Failed to search articles with status code: {search_response.status_code}")
            break
        
        search_result = ET.fromstring(search_response.content)
        ids = search_result.findall(".//Id")
        
        if not ids:
            break
        
        uids = [id_elem.text for id_elem in ids]
        all_uids.extend(uids)
        
        if len(ids) < max_results:
            break
        
        start += max_results
        rate_limit_wait()

    batch_size = 200
    for i in range(0, len(all_uids), batch_size):
        uid_batch = all_uids[i:i + batch_size]
        uid_list = ",".join(uid_batch)
        fetch_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={uid_list}&retmode=xml"
        article_data = fetch_articles_batch(fetch_url)
        
        if article_data:
            root = ET.fromstring(article_data)
            
            new_rows = []
            for article in root.findall(".//PubmedArticle"):
                title = article.find(".//ArticleTitle").text if article.find(".//ArticleTitle") is not None else ""
                
                authors = []
                affiliations = []
                for author in article.findall(".//AuthorList/Author"):
                    last_name = author.find("LastName").text if author.find("LastName") is not None else ""
                    fore_name = author.find("ForeName").text if author.find("ForeName") is not None else ""
                    authors.append(f"{last_name} {fore_name}".strip())

                    for affiliation in author.findall(".//Affiliation"):
                        affiliation_text = affiliation.text if affiliation.text else ""
                        if affiliation_text and "University of Mississippi" in affiliation_text:
                            affiliations.append(affiliation_text)

                pmc_id = article.find(".//PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
                
                pub_date_elem = article.find(".//PubDate")
                if pub_date_elem is not None:
                    year = pub_date_elem.find("Year").text if pub_date_elem.find("Year") is not None else ""
                    month = pub_date_elem.find("Month").text if pub_date_elem.find("Month") is not None else ""
                    day = pub_date_elem.find("Day").text if pub_date_elem.find("Day") is not None else ""
                    pub_date = f"{year}-{month}-{day}" if year else ""
                else:
                    pub_date = ""

                article_url = article.find(".//ELocationID[@EIdType='doi']")
                pdf_url = 'https://pubs.acs.org/doi/epdf/' + article_url.text if article_url is not None and article_url.text else ""

                if affiliations and pmc_id is not None:
                    keywords = article.findall(".//KeywordList/Keyword")
                    mesh_headings = article.findall(".//MeshHeadingList//DescriptorName")

                    keyword_texts = [kw.text for kw in keywords if kw.text]
                    mesh_headings_texts = [mh.text for mh in mesh_headings if mh.text]

                    new_row = [title, pub_date, ', '.join(authors), '; '.join(affiliations), ', '.join(keyword_texts), ', '.join(mesh_headings_texts), pdf_url, 'PubMed']
                    new_rows.append(new_row)
                
            if new_rows:
                new_df = pd.DataFrame(new_rows, columns=pubmed_df.columns)
                pubmed_df = pd.concat([pubmed_df, new_df], ignore_index=True)

    return pubmed_df

# Function for SCOAP3 API
def call_scoap3_api_call():
    columns = ['title', 'created', 'article_id', 'authors', 'affiliations', 'source']
    scoap3_df = pd.DataFrame(columns=columns)
    total_rows = []
    
    for i in range(1, 54):  # Adjust the range as needed
        print(f"Fetching page {i}")
        fetch_url = f'http://repo.scoap3.org/api/records/?sort=-date&q=university+of+mississippi&page={i}&size=10'
        article_data = fetch_articles_batch(fetch_url)
        article_data_json = json.loads(article_data)
        data_bucket = article_data_json["hits"]
        data_bucket_hits = data_bucket["hits"]
        for data in data_bucket_hits:
            total_rows.append(data)
        print(f"Total rows collected: {len(total_rows)}")
    
    articles_list = []
    
    for row in total_rows:
        created_date = row.get('created', '')
        article_id = row.get('id', '')
        
        metadata = row.get('metadata', {})
        
        title = metadata.get('title', [''])[0]
        auth = metadata.get('authors', [])
        authors = [x.get('full_name', '') for x in auth]
        affiliations = [affiliation.get('value', '') for x in auth for affiliation in x.get('affiliations', [])]

        articles_list.append({
            'title': title,
            'created': created_date,
            'article_id': article_id,
            'authors': ', '.join(authors),
            'affiliations': ', '.join(affiliations),
            'source': 'SCOAP3'
        })
    
    scoap3_df = pd.DataFrame(articles_list, columns=columns)
    
    return scoap3_df

# Fetch data from both sources
pubmed_df = call_pubmed_api_call()
scoap3_df = call_scoap3_api_call()

# Combine and process DataFrames
def combine_dataframes(df1, df2):
    # Normalize column names
    df1 = df1.rename(columns=lambda x: x.strip().lower())
    df2 = df2.rename(columns=lambda x: x.strip().lower())

    # Ensure the 'title' column exists in both DataFrames for comparison
    if 'title' not in df1.columns:
        df1['title'] = ''
    if 'title' not in df2.columns:
        df2['title'] = ''

    # Union of all fields from both dataframes
    union_df = pd.concat([df1, df2], ignore_index=True).drop_duplicates()

    # Find unique entries based on titles
    unique_df = pd.concat([df1, df2, df1.merge(df2, on='title', how='inner')]).drop_duplicates(keep=False)

    # Find intersection entries based on titles
    intersection_df = df1.merge(df2, on='title', how='inner')

    return union_df, unique_df, intersection_df

union_df, unique_df, intersection_df = combine_dataframes(pubmed_df, scoap3_df)

# Save DataFrames to Excel
with pd.ExcelWriter('combined_output.xlsx') as writer:
    union_df.to_excel(writer, sheet_name='Union', index=False)
    unique_df.to_excel(writer, sheet_name='Unique', index=False)
    intersection_df.to_excel(writer, sheet_name='Intersection', index=False)

print("DataFrames have been saved to 'combined_output.xlsx'.")
