# BuildBasket

BuildBasket is a data-driven basketball player analytics and projection tool designed to analyze, visualize, and predict player performance based on historical data. The app provides insights into a player's current attributes, tracks their development over time, and compares them to other players in the league. It includes a **Spring Boot backend** and a **Node.js frontend**. Follow the steps below to set up and run the app on your local machine.

## Requirements

Before starting the app, make sure you have the following installed:

- **Java 11+** (for the Spring Boot backend)
- **Node.js 14+** (for the frontend)
- **Maven** (for building the backend)

## Running the Project

### 1. **Clone the Repository**

First, clone the repository to your local machine:
```bash
git clone https://github.com/manishkharthik/build-basket
```

### 2. **Set up the backend**

Navigate to the backend directory `buildbasket-api` and run the Spring Boot application:
```bash
cd buildbasket-api/
./mvnw spring-boot:run
```
This will start the Spring Boot API, which runs on the default port `localhost:8080`

### 3. **Set up the frontend**

Open a new terminal tab/window (or split your terminal) and navigate to the frontend directory
```bash
cd ../buildbasket-frontend
```
Install the required dependencies for the frontend:
```bash
npm install
```
Once the dependencies are installed, you can start the development server:
```bash
npm run dev
```
This will start the frontend on the default port `localhost:3000`

### 4. **Access the Application**

Backend: The backend API will be available at `http://localhost:8080`

Frontend: The frontend will be available at `http://localhost:3000`

You should now be able to interact with both the frontend and backend locally!

## Key features
1. Player Archetype Identification: Players are categorized into archetypes (e.g., Low-Usage Playmakers, Scoring Wings, Rim Protectors) based on their statistical profile. This was done by clustering player feature vectors, which represent their playstyles in an 8-dimensional space using PCA and K-means clustering.
2. Progression Tracking: The app models a player's attribute progression over the next 5 years, forecasting improvements or declines in metrics like shooting, playmaking, and efficiency. XGBoost models are used to predict these changes based on age, role, and historical performance.
3. Comparative Analytics: Users can compare players' current performance and projected progression through interactive radar charts and detailed stats. The app also provides a nearest-neighbor search to compare players based on similar attributes.
4. Visualizations: The app generates interactive spider charts to visualize player performance across various metrics, including efficiency, impact, and defense.

## Tech Stack
- Frontend: React and communicates with the backend through Spring Boot to fetch player data, projections, and comparisons.
- Backend: Spring Boot and Supabase as the database, where all player stats and projections are stored.
- Machine Learning: Python (for data collection, cleaning, and clustering), XGBoost (for player projections)
- Data Sources: NBA API, Kaggle datasets, Basketball Reference. The app used training data from multiple sources, including the NBA API and Kaggle datasets, to gather over 15 years of player stats. This data includes basic box score stats, advanced metrics, and season-by-season summaries.








