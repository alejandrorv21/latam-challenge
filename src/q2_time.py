from typing import List, Tuple
import pandas as pd
import sys
import emoji
from memory_profiler import profile

@profile
def q2_time(file_path: str) -> List[Tuple[str, int]]:
    # Leer el alrchivo y convertirlo a Dataframe
    df = pd.read_json(file_path, lines=True)

    # Inicializar un diccionario que se encargará de contar los emoticons dentro del contenido
    cont_emojis = {}
    for content in df['content']:
        # Se obtienen los diferentes emojis que se encuentren en el contenido
        emojis = emoji.distinct_emoji_list(content)
        # Se recorren los emojis encontrados
        for emoj in emojis:
            #Se cuenta el array en caso de que ya exista, sino se inicializa en 1
            if emoj in cont_emojis:
                cont_emojis[emoj] += 1
            else:
                cont_emojis[emoj] = 1

    # Convierte el diccionario en un Dataframe
    emojis_df = pd.DataFrame(list(cont_emojis.items()), columns=['Emoji', 'Cant'])

    # Se ordena el dataframe por Cantidad de Mayor a Menor y se obtienen los primeros 10
    top_emojis = emojis_df.sort_values(by='Cant', ascending=False).head(10)

    # Retorna el resultado correspondiente
    return top_emojis.values.tolist()

if __name__ == "__main__":
    file_path = sys.argv[1]
    print(q2_time(file_path))