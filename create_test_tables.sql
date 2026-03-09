USE TestDB;
GO

CREATE TABLE Users (
    Id INT PRIMARY KEY IDENTITY(1,1),
    Username NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100),
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    IsActive BIT DEFAULT 1
);

CREATE TABLE Products (
    Id INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100) NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    Description NVARCHAR(MAX),
    CreatedAt DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE Orders (
    Id INT PRIMARY KEY IDENTITY(1,1),
    UserId INT NOT NULL,
    OrderDate DATETIME2 DEFAULT GETDATE(),
    TotalAmount DECIMAL(18,2),
    Status NVARCHAR(20)
);

CREATE TABLE OrderItems (
    Id INT PRIMARY KEY IDENTITY(1,1),
    OrderId INT NOT NULL,
    ProductId INT NOT NULL,
    Quantity INT DEFAULT 1,
    Price DECIMAL(18,2)
);

ALTER TABLE Orders ADD CONSTRAINT FK_Orders_Users FOREIGN KEY (UserId) REFERENCES Users(Id);
ALTER TABLE OrderItems ADD CONSTRAINT FK_OrderItems_Orders FOREIGN KEY (OrderId) REFERENCES Orders(Id);
ALTER TABLE OrderItems ADD CONSTRAINT FK_OrderItems_Products FOREIGN KEY (ProductId) REFERENCES Products(Id);

CREATE INDEX IX_Orders_UserId ON Orders(UserId);
CREATE INDEX IX_OrderItems_OrderId ON OrderItems(OrderId);
CREATE INDEX IX_Orders_OrderDate ON Orders(OrderDate);

INSERT INTO Users (Username, Email, CreatedAt, IsActive) VALUES
('user1', 'user1@test.com', '2024-01-01', 1),
('user2', 'user2@test.com', '2024-01-02', 1),
('user3', 'user3@test.com', '2024-01-03', 0);

INSERT INTO Products (Name, Price, Description, CreatedAt) VALUES
('Product A', 10.50, 'Description A', '2024-01-01'),
('Product B', 25.00, 'Description B', '2024-01-02'),
('Product C', 99.99, 'Description C', '2024-01-03');

INSERT INTO Orders (UserId, OrderDate, TotalAmount, Status) VALUES
(1, '2024-01-15', 35.50, 'Completed'),
(2, '2024-01-16', 25.00, 'Pending');

INSERT INTO OrderItems (OrderId, ProductId, Quantity, Price) VALUES
(1, 1, 2, 10.50),
(1, 2, 1, 14.50),
(2, 2, 1, 25.00);
