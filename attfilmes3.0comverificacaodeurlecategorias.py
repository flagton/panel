import re
import sqlite3
import mysql.connector
from mysql.connector import Error
import requests
import time
import json

TMDB_API_KEY = "f506744105e4f26570a15bc18ffe2abb"

def connect_to_db(db_type='sqlite'):
    if db_type == 'sqlite':
        db_name = input("Digite o nome do banco de dados SQLite (exemplo: example.db): ")
        conn = sqlite3.connect(db_name)
    elif db_type == 'mysql':
        host = input("Digite o host do banco de dados MySQL (exemplo: localhost): ")
        database = input("Digite o nome do banco de dados MySQL: ")
        user = input("Digite o nome do usuário do banco de dados MySQL: ")
        password = input("Digite a senha do banco de dados MySQL: ")
        conn = mysql.connector.connect(host=host, database=database, user=user, password=password, buffered=True)
    else:
        raise ValueError("Tipo de banco de dados não suportado")
    
    return conn

def list_bouquets(cursor):
    try:
        cursor.execute("SELECT id, bouquet_name FROM bouquets")
        bouquets = cursor.fetchall()
        return bouquets
    except (sqlite3.Error, Error) as error:
        print(f"Erro ao listar bouquets: {error}")
        return []

def clean_movie_name(movie_name):
    # Remove year and "legendado" indication
    movie_name = re.sub(r'\b(2018|2019|2020|2021|2022|2023|2024)\b', '', movie_name)
    movie_name = re.sub(r'\b(L)\b', '', movie_name)
    movie_name = re.sub(r'\s*\[.*?\]|\s*\(.*?\)', '', movie_name).strip()
    return movie_name

def fetch_tmdb_data(movie_name):
    cleaned_name = clean_movie_name(movie_name)
    search_url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&query={cleaned_name}&language=pt-BR"
    search_response = requests.get(search_url)
    search_data = search_response.json()

    if search_data['results']:
        movie_id = search_data['results'][0]['id']
        movie_details_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}&append_to_response=credits&language=pt-BR"
        movie_details_response = requests.get(movie_details_url)
        movie_details = movie_details_response.json()

        director = ", ".join([crew['name'] for crew in movie_details.get('credits', {}).get('crew', []) if crew['job'] == 'Director'])
        country = ", ".join([country['name'] for country in movie_details.get('production_countries', [])])
        genre = ", ".join([genre['name'] for genre in movie_details.get('genres', [])])
        poster_path = movie_details.get('poster_path', "")
        backdrop_path = movie_details.get('backdrop_path', "")
        overview = movie_details.get('overview', "")
        release_date = movie_details.get('release_date', "")
        year = release_date.split("-")[0] if release_date else None

        movie_properties = {
            "kinopoisk_url": f"https://www.themoviedb.org/movie/{movie_id}",
            "tmdb_id": movie_details['id'],
            "name": movie_details['title'],
            "o_name": movie_details['original_title'],
            "cover_big": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else "",
            "movie_image": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else "",
            "backdrop_path": [f"https://image.tmdb.org/t/p/w500{backdrop_path}"] if backdrop_path else [],
            "release_date": release_date,
            "director": director,
            "country": country,
            "genre": genre,
            "plot": overview
        }
        return json.dumps(movie_properties), year
    return None, None

def insert_category(cursor, conn, category_name):
    try:
        cursor.execute("SELECT id FROM streams_categories WHERE category_name = %s", (category_name,))
        existing_category = cursor.fetchone()
        
        if existing_category:
            print(f"Categoria '{category_name}' já existe.")
            return existing_category[0]

        cursor.execute("INSERT INTO streams_categories (category_type, category_name, parent_id, cat_order, is_adult) VALUES (%s, %s, %s, %s, %s)", ('movie', category_name, 0, 99, 0))
        conn.commit()
        cursor.execute("SELECT LAST_INSERT_ID()")
        category_id = cursor.fetchone()[0]
        print(f"Categoria '{category_name}' inserida com sucesso.")
        return category_id
    except (sqlite3.Error, Error) as error:
        print(f"Erro ao inserir categoria '{category_name}' no banco de dados: {error}")
        return None

def insert_movie(cursor, conn, movie_data, bouquet_id):
    try:
        cursor.execute("SELECT id FROM streams WHERE stream_display_name = %s AND category_id = %s AND stream_source = %s", (movie_data['stream_display_name'], movie_data['category_id'], json.dumps(movie_data['stream_source'])))
        existing_movie = cursor.fetchone()
        
        if existing_movie:
            print(f"Filme '{movie_data['stream_display_name']}' já existe na categoria '{movie_data['category_name']}' com a mesma fonte de fluxo. Pulando...")
            return
        
        cursor.execute("""
            INSERT INTO streams (
                type, category_id, stream_display_name, stream_source, movie_properties, year, direct_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            2, f"{movie_data['category_id']}", movie_data['stream_display_name'], json.dumps(movie_data['stream_source']),
            movie_data['movie_properties'], movie_data['year'], 1
        ))
        conn.commit()
        print(f"Filme '{movie_data['stream_display_name']}' inserido com sucesso.")

        # Obter o ID do filme inserido
        cursor.execute("SELECT LAST_INSERT_ID()")
        last_insert_id = cursor.fetchone()[0]

        # Atualizar a coluna bouquet_movies do bouquet selecionado
        cursor.execute("SELECT bouquet_movies FROM bouquets WHERE id = %s", (bouquet_id,))
        bouquet_movies = cursor.fetchone()[0]
        if bouquet_movies:
            bouquet_movies = json.loads(bouquet_movies)
        else:
            bouquet_movies = []
        bouquet_movies.append(last_insert_id)
        cursor.execute("UPDATE bouquets SET bouquet_movies = %s WHERE id = %s", (json.dumps(bouquet_movies), bouquet_id))
        conn.commit()
    except (sqlite3.Error, Error) as error:
        print(f"Erro ao inserir filme '{movie_data['stream_display_name']}' no banco de dados: {error}")

def get_m3u_categories(m3u_url):
    response = requests.get(m3u_url)
    lines = response.text.splitlines()
    categories = set()

    for line in lines:
        if line.startswith("#EXTINF"):
            group_title_match = re.search(r'group-title="([^"]+)"', line)
            if group_title_match:
                group_title = group_title_match.group(1).strip()
                if 'series' not in group_title.lower():
                    categories.add(group_title)

    return list(categories)

def process_m3u(m3u_url, cursor, conn, selected_categories, bouquet_id, adult_bouquet_id):
    response = requests.get(m3u_url)
    lines = response.text.splitlines()

    adult_keywords = ["porno", "Porno", "adultos", "Adultos", "xxx", "+18"]

    for i in range(len(lines)):
        if lines[i].startswith("#EXTINF"):
            line = lines[i]
            next_line = lines[i + 1] if (i + 1) < len(lines) else None

            if 'series' in line.lower() or (next_line and 'series' in next_line.lower()):
                print(f"O link '{next_line}' não pode ser inserido porque é uma série, não um filme. Pulando para a próxima categoria.")
                continue
            
            group_title_match = re.search(r'group-title="([^"]+)"', line)
            if group_title_match:
                group_title = group_title_match.group(1).strip()
            else:
                continue

            # Verificar se a categoria foi selecionada pelo usuário
            if "todas" not in selected_categories and group_title not in selected_categories:
                continue

            movie_name_match = re.search(r'tvg-name="([^"]+)"', line)
            if movie_name_match:
                movie_name = movie_name_match.group(1).strip()
            else:
                print(f"Não foi possível extrair o nome do filme de: {line}")
                continue

            stream_source = next_line

            # Verificar se a URL do filme já existe
            cursor.execute("SELECT id FROM streams WHERE REPLACE(stream_source, '\\\\/', '/') = %s", (json.dumps([stream_source]).replace('\\/', '/'),))
            existing_url = cursor.fetchone()
            if existing_url:
                print(f"URL '{stream_source}' já existe. Pulando...")
                continue

            # Inserir a categoria
            cursor.execute("SELECT id FROM streams_categories WHERE category_name = %s", (group_title,))
            category = cursor.fetchone()
            if not category:
                insert_category(cursor, conn, group_title)
                cursor.execute("SELECT id FROM streams_categories WHERE category_name = %s", (group_title,))
                category = cursor.fetchone()

            category_id = category[0]

            # Consultar dados do TMDB
            tmdb_data, year = fetch_tmdb_data(movie_name)
            if tmdb_data:
                movie_properties = tmdb_data
            else:
                movie_properties = json.dumps({
                    "kinopoisk_url": "",
                    "tmdb_id": None,
                    "name": movie_name,
                    "o_name": movie_name,
                    "cover_big": "",
                    "movie_image": "",
                    "backdrop_path": [],
                    "release_date": "",
                    "director": "",
                    "country": "",
                    "genre": "",
                    "plot": ""
                })
                year = None
            
            movie_data = {
                "category_id": f"[{category_id}]",
                "category_name": group_title,
                "stream_display_name": movie_name,
                "stream_source": [stream_source],
                "movie_properties": movie_properties,
                "year": year
            }

            # Verificar se o filme é adulto
            if any(keyword in group_title for keyword in adult_keywords):
                insert_movie(cursor, conn, movie_data, adult_bouquet_id)
            else:
                insert_movie(cursor, conn, movie_data, bouquet_id)

            time.sleep(0)  # Adiciona 1 segundo de delay

def main():
    conn = connect_to_db(db_type='mysql')  # ou 'sqlite'
    cursor = conn.cursor()
    m3u_url = input("Digite a URL da lista M3U: ")

    categories = get_m3u_categories(m3u_url)
    print("Categorias disponíveis na lista M3U:")
    for i, category in enumerate(categories):
        print(f"{i + 1}. {category}")
    print(f"{len(categories) + 1}. Todas as categorias")

    selected_option = int(input("Digite o número da categoria que deseja adicionar (ou o número para todas as categorias): "))
    selected_categories = ["todas"] if selected_option == len(categories) + 1 else [categories[selected_option - 1]]

    if conn:
        try:
            bouquets = list_bouquets(cursor)
            print("Bouquets disponíveis:")
            for bouquet in bouquets:
                print(f"{bouquet[0]} - {bouquet[1]}")

            bouquet_id = input("Digite o ID do bouquet onde deseja adicionar os filmes: ")
            bouquet_id = int(bouquet_id)

            adult_bouquet_id = input("Digite o ID do bouquet onde deseja adicionar filmes adultos: ")
            adult_bouquet_id = int(adult_bouquet_id)

            process_m3u(m3u_url, cursor, conn, selected_categories, bouquet_id, adult_bouquet_id)
        except Exception as e:
            print(f"Erro ao processar a lista M3U: {e}")
        finally:
            cursor.close()
            conn.close()
            print("Conexão ao banco de dados fechada.")

if __name__ == "__main__":
    main()
