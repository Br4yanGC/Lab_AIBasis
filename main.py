from chroma import *

def console():
    print("----------------------------------------------------------")
    print("Insert the query you wanna consult: \n> ", end='')
    query = input()
    print("Insert the number of results associated to you wanna get: \n> ", end='')
    n_results = int(input())
    print("----------------------------------------------------------")
    client, movies_collection = initialize_chroma()
    query_chroma(movies_collection, query, n_results)

console()