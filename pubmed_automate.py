import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import time

# Define the function to fetch articles in batches
def fetch_articles_batch(uids):
    fetch_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={uids}&retmode=xml"
    response = requests.get(fetch_url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to fetch articles with status code: {response.status_code}")
        return None

# Define a function to handle rate limiting
def rate_limit_wait():
    time.sleep(1)  # Adjust this sleep time based on API rate limits

# Calculate the date range for the last 20 years
current_year = datetime.now().year
start_year = current_year - 5
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
max_results = 2000  # Adjust as needed (PubMed often supports up to 2000 per request)

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
    
    # Update start for next batch
    start += max_results
    rate_limit_wait()

# Initialize counter for open access articles
open_access_count = 0

# Process UIDs in smaller batches
batch_size = 200  # Adjust based on maximum allowed batch size
for i in range(0, len(all_uids), batch_size):
    uid_batch = all_uids[i:i + batch_size]
    uid_list = ",".join(uid_batch)
    article_data = fetch_articles_batch(uid_list)
    
    if article_data:
        root = ET.fromstring(article_data)
        
        # Parse article details
        for article in root.findall(".//PubmedArticle"):
            title = article.find(".//ArticleTitle").text if article.find(".//ArticleTitle") is not None else "No title"
            
            # Extract and check author affiliations
            affiliated_with_university = any(
                "University of Mississippi" in (affiliation.text if affiliation.text else "")
                for author in article.findall(".//AuthorList/Author")
                for affiliation in author.findall(".//Affiliation")
            )
            
            # Check for open access (PMC ID presence)
            pmc_id = article.find(".//PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
            
            # Extract publication date
            pub_date_elem = article.find(".//PubDate")
            if pub_date_elem is not None:
                year = pub_date_elem.find("Year").text if pub_date_elem.find("Year") is not None else ""
                month = pub_date_elem.find("Month").text if pub_date_elem.find("Month") is not None else ""
                day = pub_date_elem.find("Day").text if pub_date_elem.find("Day") is not None else ""
                pub_date = f"{year}-{month}-{day}" if year else "No date available"
            else:
                pub_date = "No date available"
            
            if affiliated_with_university and pmc_id is not None:
                # Extract subject/discipline information
                keywords = article.findall(".//KeywordList/Keyword")
                mesh_headings = article.findall(".//MeshHeadingList//DescriptorName")

                # Collect subject/discipline information
                keyword_texts = [kw.text for kw in keywords if kw.text]
                mesh_headings_texts = [mh.text for mh in mesh_headings if mh.text]

                # Print article details
                print(f"Title: {title}")
                print(f"Publication Date: {pub_date}")
                print(f"PMC ID: {pmc_id.text} (Open Access)")
                
                if keyword_texts:
                    print(f"Keywords: {', '.join(keyword_texts)}")
                if mesh_headings_texts:
                    print(f"MeSH Headings: {', '.join(mesh_headings_texts)}")

                open_access_count += 1
            
            # Implement rate limiting
            rate_limit_wait()

# Print the count of open access articles
print(f"Total Open Access Articles from University of Mississippi: {open_access_count}")
