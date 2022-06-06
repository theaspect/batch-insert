CREATE TABLE IF NOT EXISTS vote(
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(256),
    birthDay DATE,
    station INT,
    time TIMESTAMP
)
-- Remove if you want to test write speed
ENGINE = MEMORY
