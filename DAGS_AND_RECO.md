# 🧭 Focus : Airflow & Recommandation IA

## 1. Orchestration Airflow (Les DAGs)
Votre pipeline est surveillé par **4 automates intelligents** (DAGs) :

| DAG | Fréquence | Rôle |
| :--- | :--- | :--- |
| **`daily_sentiment_digest`** | 24h | Génère un rapport HTML complet des sentiments de la journée. |
| **`ai_quality_auditor`** | 24h | Compare les étoiles (Score) avec la prédiction IA pour détecter les erreurs du modèle. |
| **`bad_buzz_alert`** | 1h | Déclenche une alerte si un produit reçoit trop d'avis négatifs d'un coup. |
| **`retraining_dag`** | Manuel | Prépare le pipeline pour mettre à jour le modèle d'IA. |

---

## 2. Système de Recommandation par Sentiment
Contrairement aux systèmes classiques basés uniquement sur les notes (Score), ce système utilise l'IA pour extraire la satisfaction réelle.

### Logique Algorithmique
1. **Extraction** : MongoDB agrège les données par `ProductId`.
2. **Calcul** : Un score de satisfaction est calculé selon le ratio :  
   `Satisfaction = (Positifs / Total) * 100`
3. **Filtrage de Confiance** : Seuls les produits ayant au moins **3 avis** sont éligibles (évite les classements basés sur un seul avis chanceux).

### Les Vues Dashboard
- **🏆 Top 10 Recommandés** : Produits avec le taux `Satisfaction` le plus élevé (Pépites).
- **⚠️ Alertes Qualité (Flops)** : Produits avec le taux `Déception` le plus élevé (à surveiller/corriger).

---
*Ce système permet de recommander des produits basés sur le **ressenti textuel** des clients, et non plus seulement sur une note de 1 à 5.*
