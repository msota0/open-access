import requests

# URL of the PDF file
pdf_url = 'https://arxiv.org/pdf/2404.16082'

# Send a GET request to the URL
response = requests.get(pdf_url)
print(response.content)

# Check if the request was successful
if response.status_code == 200:
    # Open a file in write-binary mode and save the content
    with open('paper.pdf', 'wb') as file:
        file.write(response.content)
    print("PDF downloaded successfully!")
else:
    print(f"Failed to download PDF. Status code: {response.status_code}")
