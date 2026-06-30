# Demo tape for ccrawl. Rendered with ascii-gif (github.com/tamnd/ascii-gif),
# which supplies the window chrome and theme; this file is just the action.
#
#   ascii-gif render docs/demo/ccrawl.tape -o docs/static/demo.gif
#
# ccrawl must be on PATH inside the recording shell.

Hide
Type "export PS1='$ ' PATH=/tmp/ccrawlbin:$PATH"
Enter
Type "clear"
Enter
Show

Sleep 600ms
Type "ccrawl crawls latest"
Sleep 800ms
Enter
Sleep 1.8s

Type "ccrawl get example.com --text"
Sleep 800ms
Enter
Sleep 2.5s

Type "ccrawl search 'example.com/*' -o url | head -5"
Sleep 800ms
Enter
Sleep 2.5s

Type "ccrawl columnar sql --tld gov --mime application/pdf --print"
Sleep 800ms
Enter
Sleep 3.5s
