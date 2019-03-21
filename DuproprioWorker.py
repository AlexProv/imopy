
import logging
import DuproprioScraper as DS
import argparse



parser = argparse.ArgumentParser()
parser.add_argument('--run', help='mode: url-scan, url-crawl, full-scan')
args = parser.parse_args()
dp_worker = DS.DuproprioScrapper(cities=DS.cities)
if args.run == 'url-scan':
    dp_worker.scan_new_urls()
elif args.run == 'url-crawl':
    dp_worker.crawl_new_urls()
elif args.run == 'full-scan':
    logging.info('gathering urls')
    dp_worker.scan_new_urls()
    logging.info('crawling urls')
    dp_worker.crawl_new_urls()
elif args.run == 'clean-up':
    logging.info('cleaning up')
    dp_worker.driver.close()
    dp_worker.clean_up()
elif args.run == 'scan-old-urls':
    dp_worker.scan_urls()
