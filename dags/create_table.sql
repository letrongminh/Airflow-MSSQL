CREATE TABLE Users (
    user_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(50),
    description NVARCHAR(255)
);
