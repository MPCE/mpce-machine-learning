# Joining up all the tables.

library(tidyverse)
library(magrittr)

# Estbalish connection to database
library(RMySQL)
manuscripts <- dbConnect(MySQL(), user="root", dbname="manuscripts", host="localhost")
info_schema <- dbConnect(MySQL(), user="root", dbname="information_schema", host="localhost")


# Get a combined list of all super_books
super_books <- manuscripts %>% # call db connection
  dbSendQuery("SELECT * FROM manuscript_books") %>% # query database
  dbFetch(n = Inf) %>% # fetch all results from connection
  as_tibble() # convert to tibble
  
new_super_books <- manuscripts %>%
  dbSendQuery("SELECT * FROM manuscripts_add_books") %>%
  dbFetch(n = Inf) %>%
  as_tibble() %>%
  select(-keywords, -parisian_keyword) # these columns are empty

all_super_books <- full_join(super_books, new_super_books, by="super_book_code")

# There are some duplicated titles and duplicated illegality information...
all_super_books %>%
  group_by(super_book_code) %>%
  filter(n() > 2) %>%
  select(super_book_title.x, super_book_title.y) %>%
  print(width = Inf)

# Combine the title fields into one
all_super_books %<>%
  mutate(super_book_title = coalesce(super_book_title.x, super_book_title.y))

# all_super_books %<>%
#   select(-Illegality)

# Join some more tables
parisian <- manuscripts %>%
  dbSendQuery("SELECT parisian_keyword_code, ancestor1 FROM parisian_keywords") %>%
  dbFetch(n = Inf) %>%
  as_tibble() %>%
  rename(parisian_keyword = parisian_keyword_code,
         upper_level_code = ancestor1)

# Add upper-level parisian keywords to data
all_super_books %<>%
  left_join(parisian)


# Split up keywords into multiple columns
# How many columns do we need?
keyword_cols <- all_super_books %>%
  mutate(keyword_count = str_count(keywords, ",")) %>%
  .$keyword_count %>%
  max(na.rm = T) %>%
  (function(x) paste0("keyword_", seq_len(x + 1)))()
# Create new columns
all_super_books %<>%
  separate(keywords, keyword_cols, sep = ",", fill = "right")

# Join to book data
books <- manuscripts %>%
  dbSendQuery("SELECT * FROM books") %>%
  fetch(n = Inf) %>%
  as.tibble()

books_authors <- manuscripts %>%
  dbSendQuery("SELECT * FROM manuscript_books_authors") %>%
  fetch(n = Inf) %>%
  as.tibble()

# There are up to ten authors per book:
author_columns <- books_authors %>%
  group_by(book_code) %>%
  summarise(num_authors = n()) %>%
  group_by(num_authors) %>%
  summarise(num_books = n()) %>%
  .$num_authors %>%
  max() %>%
  seq_len() %>%
  (function(x) paste("author", x, sep="_"))()

# spread out the author data
books_authors %<>%
  group_by(book_code) %>%
  summarise(author_code = paste(author_code, collapse = ",")) %>% # paste authors into a string
  separate(author_code, author_columns, fill = "right") # separate them into columns

# Add authors to books table
books %<>%
  left_join(books_authors)

# Combine books and super books
all_super_books %<>%
  full_join(books)

rm(authors, books, books_authors, author_columns)

# Get transaction data
transactions <- manuscripts %>%
  dbSendQuery("SELECT * FROM transactions") %>% # query database
  dbFetch(n = Inf) %>% # fetch all results from connection
  as_tibble() %>% # to tibble
  spread(direction_of_transaction, total_number_of_volumes) %>% # seperate column for each trans type
  group_by(book_code) %>% # for each book ...
  summarise_if(is.numeric, sum, na.rm = T) # ... add up all the different transactions.

all_super_books %<>%
  left_join(transactions)

# Get full permission simple data
library(readxl)
perm_simple <- read_xlsx("data/permission_simple.xlsx") %>%
  add_column(perm_simple = T) %>%
  rename(book_code = 3) %>%
  select(book_code, perm_simple)

all_super_books %<>%
  left_join(perm_simple)

# Merge title fields in all_super_books
all_super_books %<>%
  select(-super_book_title.x, -super_book_title.y)

# Sort columns
all_super_books %<>%
  select(book_code, super_book_code, super_book_title, everything())

# Get information on banned books
banned <- manuscripts %>%
  dbSendQuery("SELECT * FROM manuscript_titles_illegal") %>%
  dbFetch(n = Inf) %>%
  as_tibble() %>%
  filter(record_status != "DELETED") %>%
  rename(super_book_code = illegal_super_book_code,
         author_1 = illegal_author_code,
         primary_author_name = illegal_author_name,
         full_book_title = illegal_full_book_title)

# Okay, the challenge. Only 63 of the books have a super_book code. Let's just merge it all in, and we can do
# deduplication later

# Author's names will be useful.
authors <- manuscripts %>%
  dbSendQuery("SELECT * FROM manuscript_authors") %>%
  dbFetch(n = Inf) %>%
  as_tibble() %>%
  rename(author_1 = author_code,
         primary_author_name = author_name)

# Add name of primary author
all_super_books %<>%
  left_join(authors)

# Add the banned books
# banned as 2018 rows
# all_super_books has 8230 rows before the join
all_super_books %<>%
  full_join(banned)

# Now it has 10248 rows... not one row was identical when full_book_title, author_1 and primary_author_name
# were compared...
# Export csv to work with dedupe in python
title_copy <- function(col_from, col_to) {
  col_to[is.na(col_to)] <- col_from[is.na(col_to)]
  return(col_to)
} 
all_super_books %>%
  # if full_book_title is null, copy the super_book_title
  mutate(full_book_title = title_copy(super_book_title, full_book_title)) %>%
  select(UUID,super_book_code,primary_author_name,full_book_title,stated_publication_years, illegal_date) %>%
  write_csv("c:/git/mpce-machine-learning/data/reduced_super_book.csv")


# Now for the estampillage.
estampillage <- manuscripts %>%
  dbSendQuery("SELECT * FROM manuscript_events") %>%
  dbFetch(n = Inf) %>%
  as_tibble() %>%
  rename(super_book_code = ID_SuperBookTitle,
         book_code = ID_EditionName) %>%
  select(-ID, -DateEntered, -EventUser,
         -ID_AgentC, -EventType, -ID_Archive,
         -EventFolioPage, -EventCitation,
         -EventNotes, -EventOther, -EventVols) %>%
  rename(stamped_place = ID_PlaceName,
         stamped_inspector = ID_AgentA,
         stamped_adjoint = ID_AgentB,
         stamped_dealer = ID_DealerName,
         stamped_copies = EventCopies,
         stamped_date = EventDate,
         stamped_article = EventArticle,
         stamped_page = EventPageStamped,
         stamped_location = EventLocation
         )

# Add this in too
all_super_books %<>%
  full_join(estampillage)


# Try and turn the keywords into a matrix...
colchange <- function(x) {paste0("keyword_",x)}
keywords <- all_super_books %>%
  select(super_book_code, keyword_1:keyword_10) %>%
  gather(kw, keyword, -super_book_code) %>%
  drop_na() %>%
  select(-kw) %>%
  group_by(super_book_code, keyword) %>%
  summarise(n = n()) %>% # collapse duplicate keywords
  mutate(n = 1) %>% # drop duplicates
  spread(keyword, n) %>%
  select(-V1) %>%
  rename_all(colchange) %>%
  rename(super_book_code = keyword_super_book_code)

# Great, now reform the keyword data in the megamatrix
all_super_books %<>%
  select(-c(4:25)) %>% # remove old keyword cols
  left_join(keywords)

write_csv(all_super_books, "data/mother_matrix_1.csv")
