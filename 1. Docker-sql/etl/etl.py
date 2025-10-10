import pandas as pd
import psycopg2

df = pd.read_csv("/data/social_media_data_1000.csv")
df.columns = df.columns.str.strip()
df = df.dropna()
df['Post_Date'] = pd.to_datetime(df['Post_Date'])

# connect to Postgres
conn = psycopg2.connect(
    host = "db",
    dbname = "pipeline_db",
    user = "pipeline_user",
    password = "7762117689"
)
cur = conn.cursor()

# Create table if not exists
cur.execute( """
CREATE TABLE IF NOT EXISTS social_media_posts (
    Post_ID INT PRIMARY KEY,
    User_ID INT,
    Age INT,
    Gender TEXT,
    Post_Content TEXT,
    Likes INT,
    Shares INT,
    Comments INT,
    Post_Date TIMESTAMP
);
""")

# Insert data 
for _, row in df.iterrows():
    cur.execute(
        """
        INSERT INTO social_media_posts (Post_ID, User_ID, Age, Gender, Post_Content, Likes, Shares, Comments, Post_Date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (row["Post_ID"], row["User_ID"], row["Age"], row["Gender"], row["Post_Content"], row["Likes"], row["Shares"], row["Comments"], row["Post_Date"])
    )

conn.commit()
cur.close()
conn.close()
print("ETL job finished!")