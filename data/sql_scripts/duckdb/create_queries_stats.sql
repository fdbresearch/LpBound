-- Users
CREATE TABLE users (
Id INTEGER PRIMARY KEY,
Reputation INTEGER ,
CreationDate DATETIME,
Views INTEGER ,
UpVotes INTEGER ,
DownVotes INTEGER
);

-- Posts
CREATE TABLE posts (
	Id INTEGER PRIMARY KEY,
	PostTypeId SMALLINT ,
	CreationDate DATETIME,
	Score INTEGER ,
	ViewCount INTEGER,
	OwnerUserId INTEGER,
  AnswerCount INTEGER ,
  CommentCount INTEGER ,
  FavoriteCount INTEGER,
  LastEditorUserId INTEGER
);

-- PostLinks
CREATE TABLE postLinks (
	Id INTEGER PRIMARY KEY,
	CreationDate DATETIME,
	PostId INTEGER ,
	RelatedPostId INTEGER ,
	LinkTypeId SMALLINT
);

-- PostHistory
CREATE TABLE postHistory (
	Id INTEGER PRIMARY KEY,
	PostHistoryTypeId SMALLINT ,
	PostId INTEGER ,
	CreationDate DATETIME,
	UserId INTEGER
);

-- Comments
CREATE TABLE comments (
	Id INTEGER PRIMARY KEY,
	PostId INTEGER ,
	Score SMALLINT ,
  CreationDate DATETIME,
	UserId INTEGER
);

-- Votes
CREATE TABLE votes (
	Id INTEGER PRIMARY KEY,
	PostId INTEGER,
	VoteTypeId SMALLINT ,
	CreationDate DATETIME,
	UserId INTEGER,
	BountyAmount SMALLINT
);

-- Badges
CREATE TABLE badges (
	Id INTEGER PRIMARY KEY,
	UserId INTEGER ,
	Date DATETIME);

-- Tags
CREATE TABLE tags (
	Id INTEGER PRIMARY KEY,
	Count INTEGER ,
	ExcerptPostId INTEGER
);
