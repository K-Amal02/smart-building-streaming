from pymongo import MongoClient
import datetime

def test_mongodb_connection():
    """Teste la connexion à MongoDB"""
    try:
        # Connexion à MongoDB
        client = MongoClient(
            'mongodb://localhost:27017/',
            serverSelectionTimeoutMS=5000
        )
        
        # Test de connexion
        client.admin.command('ismaster')
        print("✅ Connexion MongoDB réussie!")
        
        # Création d'une base de test
        db = client['smart_buildings']
        collection = db['test_collection']
        
        # Insertion d'un document test
        test_doc = {
            "test_message": "Hello MongoDB!",
            "timestamp": datetime.datetime.now(),
            "building_id": "test_building"
        }
        
        result = collection.insert_one(test_doc)
        print(f"✅ Document inséré avec ID: {result.inserted_id}")
        
        # Lecture du document
        doc = collection.find_one({"_id": result.inserted_id})
        print(f"✅ Document lu: {doc}")
        
        # Nettoyage
        collection.delete_one({"_id": result.inserted_id})
        print("✅ Test document supprimé")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connexion MongoDB: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Test de l'infrastructure MongoDB...")
    test_mongodb_connection()
    print("🎉 Test MongoDB terminé!")