from dw_benchmark.ddl.shared.models import TCPDS100g, TCPDS100gUnoptimized
from dw_benchmark.ddl.shared.utils import run_warmup
from dw_benchmark.ddl.shared.utils import create_user_with_permissions
from dw_benchmark.ddl.redshift_ddls.redshift_utils import get_redshift_engine
from dw_benchmark.ddl.redshift_ddls.redshift_utils import run_copy_commands
from dw_benchmark.ddl.redshift_ddls.redshift_utils import run_ddl
from dw_benchmark.ddl.redshift_ddls.redshift_utils import run_analyze
import config


engine = get_redshift_engine(
    host=config.host, db=config.db, user=config.user, pwd=config.pw
)

for model in [TCPDS100g, TCPDS100gUnoptimized]:
    run_ddl(TCPDS100g, engine=engine)
    run_copy_commands(TCPDS100gUnoptimized, engine=engine, role=config.role)
    run_analyze(engine)
    run_warmup(TCPDS100gUnoptimized, engine=engine)
    run_warmup(TCPDS100g, engine=engine)
    create_user_with_permissions("tcpds", engine=engine)
