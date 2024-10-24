import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

# Cabeçalhos globais para as requisições
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

MAX_THREADS = 20

def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))  # Evita sobrecarregar o servidor
    response = requests.get(movie_link, headers=headers)
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    if movie_soup is not None:
        title, date, rating, plot_text = None, None, None, None
        
        # Encontrando a seção do título
        title_tag = movie_soup.find('h1')
        if title_tag:
            title = title_tag.get_text().strip()
        
        # Encontrando a data de lançamento
        date_tag = movie_soup.find('a', href=lambda href: href and 'releaseinfo' in href)
        if date_tag:
            date = date_tag.get_text().strip()
        
        # Encontrando a classificação do filme
        rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        if rating_tag:
            rating = rating_tag.get_text().strip()
        
        # Encontrando a sinopse do filme
        plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
        if plot_tag:
            plot_text = plot_tag.get_text().strip()
        
        return title, date, rating, plot_text
    return None, None, None, None

def extract_movies(soup):
    # Selecionando a lista de filmes populares
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'})
    movies_table_rows = movies_table.find_all('li')

    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    movie_data = []

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(extract_movie_details, movie_links)
        for result in results:
            if all(result):  # Apenas incluir se todos os dados estiverem presentes
                movie_data.append(result)

    # Escrevendo os dados dos filmes no arquivo CSV
    with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for data in movie_data:
            movie_writer.writerow(data)

def main():
    start_time = time.time()

    # URL para filmes populares do IMDB
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extração dos filmes
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

if __name__ == '__main__':
    main()