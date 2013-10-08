import config
import lee

lee.connect(config.sqlite_path)

from models import main
main()
