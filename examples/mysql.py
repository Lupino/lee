import config
import lee

lee.connect(config.mysql_path)

from models import main
main()
