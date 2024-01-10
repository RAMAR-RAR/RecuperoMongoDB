from bson import ObjectId
from pymongo import MongoClient


#-------------------------------------

# Scrivi una query che restituisca tutti i documenti della collection.

def ex1_find_all():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {

    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)

    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i nomi e le città dei ristoranti.
def ex2_find_all_show_name_city():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {

    }

    #query di scelta campi da mostrare
    projection = {
        "nome": 1,
        "città": 1,
        "_id": 0
    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno almeno due stelle.
        
        #Nota: attualmente la collection contiene solo ristoranti da 3 stelle

def ex3_find_only_2_stars_greatE():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "stelle":{"$gte":2}
    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno la cucina italiana tra le loro specialità.
        
        #Nota: In senso generale sono solo due che propongono cucina italiana, altri ristoranti
        # sono focalizzati su una cucina italiana regionale

def ex4_find_only_italian():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "cucina":{"$in":["italiana"]}

    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno un prezzo inferiore a 250 euro.


def ex5_find_only_250_euros_less():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "prezzo":{"$lt":250}
    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()
#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno la cucina sperimentale o innovativa tra le loro specialità.

def ex6_find_only_sperimental_innovative():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "cucina":{"$in":["sperimentale", "innovativa"]}

    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno la cucina italiana tra le loro specialità e che si trovano a Roma o a Firenze.

def ex7_find_only_italian_in_rome_florence():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "$and":[{"cucina":{"$in":["italiana"]}}, {"città":{"$in":["Roma", "Firenze"]}}]
    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno tre stelle e che hanno un prezzo superiore a 250 euro.

def ex8_find_only_3_stars_250_euros_great():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "$and":[{"stelle":3}, {"prezzo":{"$gt":250}}]
    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca solo i ristoranti che hanno la cucina alpina o mare tra le loro specialità e che si trovano in una città che inizia con la lettera S.

def ex9_find_only_alpine_maritime_start_with_S():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {
        "$and":[{"città": {"$regex": "^S"}, "cucina": {"$in": ["alpina", "mare"]}}]
    }

    #query di scelta campi da mostrare
    projection = {

    }

    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.find(
            filter=query,
            projection=projection
            ):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()

#-------------------------------------

# Scrivi una query che restituisca il numero di ristoranti per ogni città, ordinati in ordine decrescente

def ex10_most_resturant_in_city_sorted_desc():

    #collegamento al cluster
    client = MongoClient('mongodb+srv://mruzzi:gvCvXQLuHMW8tp1H@cluster0.rdcj9qk.mongodb.net/')

    #query di filtraggio di selezione dati
    query = {

    }

    #query di scelta campi da mostrare
    projection = {

    }

    sorting = {

    }

    pipeline= [
        {"$group": {"_id": "$città", "count": {"$sum": 1}}},{"$sort": {"count": -1}}
    ]



    # Ricerca dei document con le query specifiche
    for post in client.ITSAR.resturants.aggregate(pipeline):
        print(post)
    
    #chiusura collegamento al cluster
    client.close()


#-------------------------------------

if __name__ == "__main__":
    #Esecuzione funzioni
    ex1_find_all()