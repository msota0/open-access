import requests
import json

def fetch_all_results(base_url):
    results = []
    page = 1
    per_page = 25
    
    while True:
        print(page)
        url = f'{base_url}?page={page}&per_page={per_page}'
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            current_results = data['results']
            results.extend(current_results)
            
            # Check if we've fetched the last page
            if len(current_results) < 0:
                break
            
            page += 1
        else:
            print(f"Failed to fetch articles with status code: {response.status_code}")
            break
    
    return results

base_url = "https://api.openalex.org/works?filter=institutions.id:I368840534"
all_results = fetch_all_results(base_url)

# Print the number of results fetched
print(f"Total number of results fetched: {len(all_results)}")

# Optionally, print some of the results
# for result in all_results[:5]:  # Print the first 5 results
#     print(json.dumps(result, indent=2))


# #not working maybe

