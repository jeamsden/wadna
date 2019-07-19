Example:

>>> test = Dataset('basic')

>>> test.load()

>>> test.queued_df

   name     sex
0   Jim    male
1  Jane  female
2  Jill  female

>>> test.etl()

>>> test.published_df

   index first_name     sex
0      0        Jim    male
1      1       Jane  female
2      2       Jill  female
