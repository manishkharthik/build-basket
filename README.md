# BuildBasket

BuildBasket is a personal project that includes a **Spring Boot backend** and a **Node.js frontend**. Follow the steps below to set up and run the app on your local machine.

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





