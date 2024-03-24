from typing import List, Tuple
import pandas as pd
import sys
from memory_profiler import profile

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Leer el alrchivo y convertirlo a Dataframe
    df = pd.read_json(file_path, lines=True, chunksize=100)

    lista_usuarios = {}
    for chunk in df:
        for list_users in chunk['mentionedUsers']:
            if list_users is not None:
                for user in list_users:
                    if user['username'] in lista_usuarios:
                        lista_usuarios[user['username']] += 1
                    else:
                        lista_usuarios[user['username']] = 1

    return sorted(lista_usuarios.items(), key=lambda item: item[1], reverse=True)[:10]

if __name__ == "__main__":
    file_path = sys.argv[1]
    print(q3_memory(file_path))

## Measure-Command { python asdasdasdasd }