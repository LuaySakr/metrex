from .base import Condition, register, REGISTRY, all_conditions, get_condition_ids
from .context import build_context
from .aggregate import evaluate_all, compress_boolean_matrix

# Import condition definition files (keep incremental groups)
from . import conditions_001_020  # noqa: F401
from . import conditions_021_040  # noqa: F401
# Following groups auto-imported for registration (defined progressively)
try:
	from . import conditions_041_060  # noqa: F401
	from . import conditions_061_080  # noqa: F401
	from . import conditions_081_100  # noqa: F401
	from . import conditions_101_120  # noqa: F401
	from . import conditions_121_140  # noqa: F401
	from . import conditions_141_150  # noqa: F401
except ImportError:
	# Some groups may not yet exist; safe to ignore during partial development
	pass