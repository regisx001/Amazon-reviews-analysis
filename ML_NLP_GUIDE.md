y# 🧠 Guide ML & NLP : Analyse de Sentiment avec PySpark

Ce document détaille le cycle de vie de l'IA dans votre projet, expliquant comment nous transformons des avis clients bruts en prédictions de sentiment précises.

---

## 1. Phase de Preprocessing (`spark_preprocessing_job.py`)

C'est l'étape de préparation du terrain. Sans de bonnes données, le modèle ne peut rien apprendre.

### A. Création des Labels (Étiquetage)
L'IA a besoin de savoir ce qui est "bon" ou "mauvais". Nous convertissons le `Score` (1 à 5) en catégories :
- **1-2 étoiles** → `negative` (0)
- **3 étoiles** → `neutral` (1)
- **4-5 étoiles** → `positive` (2)

### B. Stratified Splitting (80/10/10)
Nous divisons les données en 3 jeux : **Train** (entraînement), **Val** (validation) et **Test** (évaluation finale).
- **Pourquoi "Stratifié" ?** Pour s'assurer que chaque jeu contient la même proportion d'avis positifs, neutres et négatifs que la base originale. Cela évite d'entraîner un modèle qui ne connaîtrait que les avis positifs, par exemple.

### C. Feature Fusion & Class Weights
- **Feature Fusion** : On concatène le `Summary` et le `Text` pour que l'IA ait tout le contexte. Un titre comme "Nul !" est une information cruciale.
- **Class Weights** : Les avis neutres sont rares dans les données Amazon. On leur donne plus de "poids" (8.0 contre 0.5 pour les positifs) pour forcer l'IA à y faire attention.

---

## 2. Pipeline NLP (Natural Language Processing)

Le texte est une donnée "sale" pour un ordinateur. Nous devons le transformer en nombres.

1. **RegexTokenizer** : On découpe les phrases en mots (tokens) en ignorant la ponctuation.
2. **StopWordsRemover** : On supprime les mots "vides" qui n'apportent pas de sens (`the`, `is`, `at`...), sauf les mots de négation (`not`, `no`) car ils changent totalement le sens d'un avis !
3. **Lemmatizer** : On ramène les mots à leur racine. Exemple : `running`, `runs`, `ran` → `run`. Cela simplifie le vocabulaire pour l'IA.
4. **NGram (Bigrams)** : On crée des paires de mots (ex: `not good`). Cela permet à l'IA de comprendre que `not` + `good` = négatif.
5. **HashingTF** : On transforme chaque mot/paire de mots en un nombre (un index) dans une matrice de 100 000 colonnes.
6. **IDF (Inverse Document Frequency)** : On donne plus d'importance aux mots rares et significatifs (`excellent`, `disaster`) et moins aux mots trop fréquents.

---

## 3. Training (`spark_training_job.py`)

Nous utilisons la **Logistic Regression (Multinomial)**.

- **Pourquoi ce modèle ?** C'est un excellent compromis entre vitesse et précision pour la classification de texte. Il est capable de gérer des milliers de mots simultanément de manière distribuée sur Spark.
- **Hyperparamètres** :
  - `maxIter=200` : Le modèle ré-essaie 200 fois d'améliorer sa précision.
  - `regParam=0.001` : Évite que le modèle ne "mémorise" par cœur les avis (Overfitting).

---

## 4. Evaluation (`spark_evaluation_job.py`)

On teste le modèle sur des données qu'il n'a jamais vues (**Test Set**).

### Les Métriques Clés :
- **Accuracy** : Pourcentage global de bonnes réponses.
- **Precision** : "Quand l'IA dit que c'est positif, quelle est la probabilité que ce soit vrai ?"
- **Recall** : "Sur tous les avis positifs réels, combien l'IA a-t-elle réussi à en trouver ?"
- **F1-Score** : La moyenne entre Precision et Recall. C'est la métrique la plus fiable pour juger la performance globale.

### Confusion Matrix
C'est un tableau qui montre où l'IA se trompe. Exemple : Confond-elle souvent les avis `Neutral` avec les `Positive` ? C'est l'outil ultime pour debugger le cerveau de l'IA.

---

## 5. Inference (Streaming Job)

Une fois le modèle entraîné et sauvé, il est chargé par le **Streaming Job**. Chaque avis arrivant via Kafka passe par le même pipeline NLP avant d'être classé instantanément par le modèle `LogisticRegression`.
