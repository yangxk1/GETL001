USE rm_data_5x;  -- 目标数据库（需提前创建）

-- 1. 创建数字辅助表（0-999）
DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (n INT PRIMARY KEY);
INSERT INTO numbers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

-- 2. 设置扩展倍数
SET @N = 5;  -- ← 修改这里！

-- 3. 创建目标表（结构同源库）
CREATE TABLE IF NOT EXISTS `movie` LIKE rm_data.movie;
CREATE TABLE IF NOT EXISTS `genometags` LIKE rm_data.genometags;
CREATE TABLE IF NOT EXISTS `ratings` LIKE rm_data.ratings;
CREATE TABLE IF NOT EXISTS `tags` LIKE rm_data.tags;
CREATE TABLE IF NOT EXISTS `genome-scores` LIKE rm_data.`genome-scores`;

-- 4. 扩展 movie 表（节点）
INSERT INTO `movie`
SELECT
  id * 10 + n.n AS id,
  title,
  genres,
  imdbId,
  tmdbId
FROM rm_data.movie
CROSS JOIN numbers n
WHERE n.n < @N;

-- 5. 扩展 genometags 表（节点）
INSERT INTO `genometags`
SELECT
  id * 10 + n.n AS id,
  tag
FROM rm_data.genometags
CROSS JOIN numbers n
WHERE n.n < @N;

-- 6. 扩展 ratings 表（关系：user -> movie）
INSERT INTO `ratings`
SELECT
  user * 10 + n.n AS user,
  movie * 10 + n.n AS movie,
  rating,
  timestamp
FROM rm_data.ratings
CROSS JOIN numbers n
WHERE n.n < @N;

-- 7. 扩展 tags 表（关系：user -> movie）
INSERT INTO `tags`
SELECT
  user * 10 + n.n AS user,
  movie * 10 + n.n AS movie,
  tag,
  timestamp
FROM rm_data.tags
CROSS JOIN numbers n
WHERE n.n < @N;

-- 8. 扩展 genome-scores 表（关系：movie -> genometags）
INSERT INTO `genome-scores`
SELECT
  movie * 10 + n.n AS id,
  genometags * 10 + n.n AS genometags,
  relevance
FROM rm_data.`genome-scores`
CROSS JOIN numbers n
WHERE n.n < @N;

-- 9. 验证结果
SELECT 'Movie count:', COUNT(*) FROM `movie`;
SELECT 'GenomeTags count:', COUNT(*) FROM `genometags`;
SELECT 'Ratings count:', COUNT(*) FROM `ratings`;
SELECT 'Tags count:', COUNT(*) FROM `tags`;
SELECT 'Genome-scores count:', COUNT(*) FROM `genome-scores`;