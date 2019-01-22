xterm -hold -title "Peer 1" -e "python3 cdht.py  1 3 4" &
xterm -hold -title "Peer 3" -e "python3 cdht.py  3 4 5" &
xterm -hold -title "Peer 4" -e "python3 cdht.py  4 5 8" &
xterm -hold -title "Peer 5" -e "python3 cdht.py  5 8 10" &
xterm -hold -title "Peer 8" -e "python3 cdht.py  8 10 12" &
xterm -hold -title "Peer 10" -e "python3 cdht.py  10 12 15" &
xterm -hold -title "Peer 12" -e "python3 cdht.py  12 15 1" &
xterm -hold -title "Peer 15" -e "python3 cdht.py  15 1 3" &

