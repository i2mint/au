"""``import au`` must not import the HTTP stack (fastapi, flask); the names stay reachable."""

import subprocess
import sys
import textwrap

import pytest

HTTP_NAMES = ("mk_http_interface", "create_app_from_decorator", "mk_flask_interface")


def _run(code: str) -> str:
    """Run ``code`` in a fresh interpreter (this session has already imported plenty)."""
    done = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout.strip()


def test_import_au_does_not_import_the_http_stack():
    out = _run(
        """
        import sys, au
        print(sorted(m for m in ('au.http', 'fastapi', 'flask') if m in sys.modules))
        """
    )
    assert out == "[]"


@pytest.mark.parametrize("name", HTTP_NAMES)
def test_http_names_resolve_on_first_use(name):
    import au
    import au.http

    assert getattr(au, name) is getattr(au.http, name)


def test_from_import_and_star_import_still_work():
    out = _run(
        """
        from au import mk_http_interface
        ns = {}
        exec('from au import *', ns)
        print(callable(mk_http_interface), 'mk_flask_interface' in ns)
        """
    )
    assert out == "True True"


def test_http_submodule_attribute_and_dir():
    out = _run(
        """
        import au
        print('mk_http_interface' in dir(au), au.http.__name__)
        """
    )
    assert out == "True au.http"


def test_unknown_attribute_still_raises():
    import au

    with pytest.raises(AttributeError, match="no_such_name"):
        au.no_such_name
