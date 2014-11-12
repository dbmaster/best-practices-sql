USE [master]
GO
CREATE LOGIN [existing_login] WITH PASSWORD=N'1', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
USE [DBM_Playground]
GO
CREATE USER [existing_login] FOR LOGIN [existing_login]
GO
USE [DBM_Playground]
GO
ALTER ROLE [db_datareader] ADD MEMBER [existing_login]
GO

drop login [existing_login]