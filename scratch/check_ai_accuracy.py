from pymongo import MongoClient
import os

def check_predictions():
    # Connect to Mongo
    mongo_uri = "mongodb://admin:admin123@localhost:2017/" # Port mapping to host
    # Wait, in local it's 27017 or whatever is mapped. Let's check docker-compose.
    # From previous logs it seems to be default 27017 or mapped to something.
    client = MongoClient("mongodb://admin:admin123@localhost:27017/")
    db = client["amazon_reviews"]
    col = db["predictions"]
    
    print("\n--- DIAGNOSTIC DES PRÉDICTIONS IA ---\n")
    
    # Get 10 recent predictions
    samples = list(col.find().sort("Time", -1).limit(10))
    
    if not samples:
        print("Aucune donnée trouvée dans MongoDB. Le stream tourne-t-il ?")
        return

    for i, s in enumerate(samples):
        print(f"Avis #{i+1}")
        print(f"Produit: {s.get('ProductId')}")
        print(f"Note Humaine (Stars): {s.get('Score')}/5")
        print(f"PRÉDICTION IA: {s.get('PredictedSentiment').upper()}")
        print(f"Texte: {s.get('Text')[:150]}...")
        print("-" * 40)

if __name__ == "__main__":
    check_predictions()
