import requests

def search_articles_by_affiliation(affiliation):
    # Replace with a real API call to a service that can search by affiliation
    response = requests.get(f'https://api.crossref.org/works?filter=affiliation:University%20of%20Mississippi')
    articles = response.json()['message']['items']
    return [article['DOI'] for article in articles]

def get_open_access_links(doi, email):
    response = requests.get(f'https://api.unpaywall.org/v2/{doi}?email={email}')
    return response.json()

def main():
    affiliation = 'University of Mississippi'
    email = 'msota@olemiss.com'
    
    dois = search_articles_by_affiliation(affiliation)
    for doi in dois:
        result = get_open_access_links(doi, email)
        print(f"DOI: {doi}")
        print(f"Open Access URL: {result.get('best_oa_location', {}).get('url_for_pdf', 'No PDF available')}")

if __name__ == "__main__":
    main()
