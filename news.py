def main():
    import hj as hj
    hj.main()
    import mjtt as mjtt
    mjtt.main()
    import btbtt06
    btbtt06.getNews(btbtt06.DB_URL)
    import subhd
    subhd.fetchFeedAndHeaders(subhd.DB_URL)


if __name__ == '__main__':
    main()