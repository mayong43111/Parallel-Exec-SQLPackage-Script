CREATE TABLE [dbo].[Users]
(
    UserID int NOT NULL,
    UserName nvarchar(2) NOT NULL,
    DisplayName nvarchar(255) NULL,
    CountryID int NOT NULL,
)
WITH
(
    DISTRIBUTION = HASH (UserName),
    CLUSTERED COLUMNSTORE INDEX
)
GO
