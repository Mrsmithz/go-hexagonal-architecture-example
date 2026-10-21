BEGIN {
	threshold = 90
	if (ARGC > 2) {
		threshold = ARGV[2] + 0
		ARGV[2] = ""
	}
}
NR > 1 {
	statements += $2
	if ($3 + 0 > 0) covered += $2
}
END {
	if (statements == 0) {
		print "coverage profile has no statements"
		exit 1
	}
	printf "statement coverage: %d/%d (%.4f%%), required %.4f%%\n", covered, statements, covered * 100 / statements, threshold
	if (covered * 100 < threshold * statements) exit 1
}
