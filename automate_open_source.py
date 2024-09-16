import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import time
import pandas as pd
import json

# Helper function: fetching for API calls
def fetch_articles_batch(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to fetch articles with status code: {response.status_code}")
        return None

# Function to handle rate limiting
def rate_limit_wait():
    time.sleep(1)  

# Function for PubMed API
def call_pubmed_api_call():
    columns = ['title', 'published date', 'authors', 'affiliations', 'keyword', 'meshing text', 'url']
    pubmed_df = pd.DataFrame(columns=columns)
    
    # Calculate the date range
    current_year = datetime.now().year
    start_year = current_year
    end_year = current_year

    # Define the search term and date range
    search_term = "University of Mississippi[Affiliation]"
    search_url_template = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?"
        "db=pubmed&term={search_term}&retmode=xml&mindate={start_date}&maxdate={end_date}&retstart={start}&retmax={max_results}"
    )

    # Initialize counters and results
    open_access_count = 0
    all_uids = []
    start = 0
    max_results = 2000  

    # Fetch results in batches
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
        
        # Break if fewer results than requested (last page)
        if len(ids) < max_results:
            break
        
        start += max_results
        # rate_limit_wait()

    open_access_count = 0

    # Process UIDs in smaller batches
    batch_size = 200  
    count = 0
    for i in range(0, len(all_uids), batch_size):
        uid_batch = all_uids[i:i + batch_size]
        uid_list = ",".join(uid_batch)
        fetch_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={uid_list}&retmode=xml"
        article_data = fetch_articles_batch(fetch_url)
        
        if article_data:
            root = ET.fromstring(article_data)
            
            # Parse article details
            new_rows = []
            for article in root.findall(".//PubmedArticle"):
                title = article.find(".//ArticleTitle").text if article.find(".//ArticleTitle") is not None else ""
                
                # Extract and check author affiliations
                authors = []
                affiliations = []
                for author in article.findall(".//AuthorList/Author"):
                    last_name = author.find("LastName").text if author.find("LastName") is not None else ""
                    fore_name = author.find("ForeName").text if author.find("ForeName") is not None else ""
                    authors.append(f"{last_name} {fore_name}".strip())

                    # Affiliation of the author
                    for affiliation in author.findall(".//Affiliation"):
                        affiliation_text = affiliation.text if affiliation.text else ""
                        if affiliation_text and "University of Mississippi" in affiliation_text:
                            affiliations.append(affiliation_text)

                # Check for open access (PMC ID presence)
                pmc_id = article.find(".//PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
                
                # Extract publication date
                pub_date_elem = article.find(".//PubDate")
                if pub_date_elem is not None:
                    year = pub_date_elem.find("Year").text if pub_date_elem.find("Year") is not None else ""
                    month = pub_date_elem.find("Month").text if pub_date_elem.find("Month") is not None else ""
                    day = pub_date_elem.find("Day").text if pub_date_elem.find("Day") is not None else ""
                    pub_date = f"{year}-{month}-{day}" if year else ""
                else:
                    pub_date = ""

                # Retrieve article URL
                article_url = article.find(".//ELocationID[@EIdType='doi']")
                pdf_url = 'https://pubs.acs.org/doi/epdf/' + article_url.text if article_url is not None and article_url.text else ""
                if article_url is None:
                    count += 1
                
                if affiliations and pmc_id is not None:
                    # Extract subject/discipline information
                    keywords = article.findall(".//KeywordList/Keyword")
                    mesh_headings = article.findall(".//MeshHeadingList//DescriptorName")

                    # Collect subject/discipline information
                    keyword_texts = [kw.text for kw in keywords if kw.text]
                    mesh_headings_texts = [mh.text for mh in mesh_headings if mh.text]

                    # Prepare the row for the DataFrame
                    new_row = [title, pub_date, ', '.join(authors), '; '.join(affiliations), ', '.join(keyword_texts), ', '.join(mesh_headings_texts), pdf_url]
                    new_rows.append(new_row)
                    open_access_count += 1
                
                # Implement rate limiting
                # rate_limit_wait()

            # Append new_rows to pubmed_df
            if new_rows:
                new_df = pd.DataFrame(new_rows, columns=pubmed_df.columns)
                pubmed_df = pd.concat([pubmed_df, new_df], ignore_index=True)

    # Print the count of open access articles
    print(f"Total Open Access Articles from University of Mississippi: {open_access_count}")
    return pubmed_df, open_access_count, count

# df, count, no_url = call_pubmed_api_call()
# print(df.head(5))
# print(no_url)
# df.to_excel('output.xlsx', index=False)

# Placeholder for scoap3 API call
def call_scoap3_api_call():
    columns = ['created', 'article_id', 'authors', 'affiliations']
    scoap3_df = pd.DataFrame(columns=columns)
    total_rows = []
    
    # Fetch data from multiple pages
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
    
    # Lists to store the extracted data
    articles_list = []
    
    for row in total_rows:
        # print("Row Data:", json.dumps(row, indent=2))  # Print the row to check structure
        
        created_date = row.get('created', '')
        article_id = row.get('id', '')
        
        # Check structure of metadata
        metadata = row.get('metadata', {})
        # print("Metadata:", json.dumps(metadata, indent=2))  # Print metadata to check structure
        
        # Get authors
        auth = metadata.get('authors', [])
        
        authors = []
        aff = []
        for x in auth:
            authors.append(x.get('full_name'))
            current = x.get('affiliations' , [])
            # print(type(current))
            # print(current[0])
            # aff.append(.get('value'))
            if len(current) > 0:
                affiliation = current[0]
                aff.append(affiliation.get('value'))

                       
        
        # Get affiliations
        
        # affiliations = [affiliation.get('value') for affiliation in aff[0]]
        
        # Append data to the list
        articles_list.append({
            'created': created_date,
            'article_id': article_id,
            'authors': ', '.join(authors),
            'affiliations': ', '.join(aff)
        })
    
    # Create DataFrame
    scoap3_df = pd.DataFrame(articles_list, columns=columns)
    
    # Print DataFrame for verification (optional)
    print(scoap3_df.head())
    
    return not scoap3_df.empty

# Call the function and print whether articles were retrieved
articles_retrieved = call_scoap3_api_call()
# print("Articles retrieved:", articles_retrieved)

# articles_retrieved = call_scoap3_api_call()
# print(articles_retrieved)