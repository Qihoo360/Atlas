# _AC_PATH_PROG_FEATURE_CHECK(VARIABLE, PROGNAME-LIST, FEATURE-TEST, [PATH])
# --------------------------------------------------------------------------
# FEATURE-TEST is called repeatedly with $ac_path_VARIABLE set to the
# name of a program in PROGNAME-LIST found in PATH.  FEATURE-TEST must set
# $ac_cv_path_VARIABLE to the path of an acceptable program, or else
# _AC_PATH_PROG_FEATURE_CHECK will report that no acceptable program
# was found, and abort.  If a suitable $ac_path_VARIABLE is found in the
# FEATURE-TEST macro, it can set $ac_path_VARIABLE_found=':' to accept
# that value without any further checks.
m4_define([_AC_PATH_PROG_FEATURE_CHECK],
[# Extract the first word of "$2" to use in msg output
if test -z "$$1"; then
set dummy $2; ac_prog_name=$[2]
AC_CACHE_VAL([ac_cv_path_$1],
[ac_path_$1_found=false
# Loop through the user's path and test for each of PROGNAME-LIST
_AS_PATH_WALK([$4],
[for ac_prog in $2; do
  for ac_exec_ext in '' $ac_executable_extensions; do
    ac_path_$1="$as_dir/$ac_prog$ac_exec_ext"
    AS_EXECUTABLE_P(["$ac_path_$1"]) || continue
    $3
    $ac_path_$1_found && break 3
  done
done
])
])
$1="$ac_cv_path_$1"
if test -z "$$1"; then
  AC_MSG_ERROR([no acceptable $ac_prog_name could be found in dnl
m4_default([$4], [\$PATH])])
fi
AC_SUBST([$1])
else
  ac_cv_path_$1=$$1
fi
])


# _AC_FEATURE_CHECK_LENGTH(PROGPATH, CACHE-VAR, CHECK-CMD, [MATCH-STRING])
# ------------------------------------------------------------------------
# For use as the FEATURE-TEST argument to _AC_PATH_PROG_FEATURE_TEST.
# On each iteration run CHECK-CMD on an input file, storing the value
# of PROGPATH in CACHE-VAR if the CHECK-CMD succeeds.  The input file
# is always one line, starting with only 10 characters, and doubling
# in length at each iteration until approx 10000 characters or the
# feature check succeeds.  The feature check is called at each
# iteration by appending (optionally, MATCH-STRING and) a newline
# to the file, and using the result as input to CHECK-CMD.
m4_define([_AC_FEATURE_CHECK_LENGTH],
[# Check for GNU $1 and select it if it is found.
  _AC_PATH_PROG_FLAVOR_GNU([$$1],
    [$2="$$1" $1_found=:],
  [ac_count=0
  echo $ECHO_N "0123456789$ECHO_C" >"conftest.in"
  while :
  do
    cat "conftest.in" "conftest.in" >"conftest.tmp"
    mv "conftest.tmp" "conftest.in"
    cp "conftest.in" "conftest.nl"
    echo '$4' >> "conftest.nl"
    $3 < "conftest.nl" >"conftest.out" 2>/dev/null || break
    diff "conftest.out" "conftest.nl" >/dev/null 2>&1 || break
    ac_count=`expr $ac_count + 1`
    if test $ac_count -gt ${$1_max-0}; then
      # Best one so far, save it but keep looking for a better one
      $2="$$1"
dnl   # Using $1_max so that each tool feature checked gets its
dnl   # own variable.  Don't reset it otherwise the implied search
dnl   # for best performing tool in a list breaks down.
      $1_max=$ac_count
    fi
    # 10*(2^10) chars as input seems more than enough
    test $ac_count -gt 10 && break
  done
  rm -f conftest.in conftest.tmp conftest.nl conftest.out])
])


# _AC_PATH_PROG_FLAVOR_GNU(PROGRAM-PATH, IF-SUCCESS, [IF-FAILURE])
# ----------------------------------------------------------------
m4_define([_AC_PATH_PROG_FLAVOR_GNU],
[# Check for GNU $1
case `"$1" --version 2>&1` in
*GNU*)
  $2;;
m4_ifval([$3],
[*)
  $3;;
])esac
])# _AC_PATH_PROG_FLAVOR_GNU

# AC_PROG_SED
# -----------
# Check for a fully functional sed program that truncates
# as few characters as possible.  Prefer GNU sed if found.
AC_DEFUN([AC_PROG_SED],
[AC_CACHE_CHECK([for a sed that does not truncate output], ac_cv_path_SED,
    [dnl ac_script should not contain more than 99 commands (for HP-UX sed),
     dnl but more than about 7000 bytes, to catch a limit in Solaris 8 /usr/ucb/sed.
     ac_script=s/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/
     for ac_i in 1 2 3 4 5 6 7; do
       ac_script="$ac_script$as_nl$ac_script"
     done
     echo "$ac_script" | sed 99q >conftest.sed
     $as_unset ac_script || ac_script=
     _AC_PATH_PROG_FEATURE_CHECK(SED, [sed gsed],
        [_AC_FEATURE_CHECK_LENGTH([ac_path_SED], [ac_cv_path_SED],
                ["$ac_path_SED" -f conftest.sed])])])
 SED="$ac_cv_path_SED"
 AC_SUBST([SED])dnl
 rm -f conftest.sed
])# AC_PROG_SED
