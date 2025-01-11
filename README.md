# SBHacksXI

### Data Sources for Celebrity News
- You can use the following data sources to gather news about celebrities:
- News APIs: Use APIs like NewsAPI, GDELT, or The Guardian API to fetch news articles.
- RSS Feeds: Subscribe to RSS feeds from celebrity news websites like TMZ, People, or E! Online.
- Web Scraping: Use web scraping tools (e.g., BeautifulSoup, Scrapy) to extract news articles from websites.
- Social Media: Collect data from platforms like Twitter, Instagram, or Reddit using their APIs.
- Public Datasets: Use datasets like Kaggle's Celebrity News Dataset or other open datasets.

### Pipeline Overview
1. The pipeline consists of the following steps:
2. Data Ingestion: Collect raw news data from the sources mentioned above.
3. Data Parsing and Preprocessing: Use Aryn's docParser and DocPrep to parse and clean the data.
4. Embedding Generation: Convert the cleaned text into vector embeddings.
5. Vector Database Storage: Store the embeddings and metadata in SingleStore.
6. Querying and Retrieval: Use SingleStore to retrieve relevant news articles for a given query.
7. RAG Integration: Pass the retrieved articles to the RAG model for generating responses.

