Kolokvijum1, u toku pokretanja programa vidjece se kako radi, a reportovi tj .xml fajlovi ce se nalaziti na putanji bin->Debug->net8.0->reports, a da se vidi line code coverage treba odraditi u powershell-u sledece komande: \n
dotnet test -c Debug --collect:"XPlat Code Coverage" \n
reportgenerator -reports:"C:\Users\User\Desktop\E2 RUS\Semestar VIII\PSUSU\Vjezbe\Kolokvijum1\Kolokvijum1.Tests\coverage.cobertura.xml" -targetdir:"coverage-report" -reporttypes:"TextSummary" \n
type .\coverage-report\Summary.txt \n 
, nakon cega se dobije ispis koji ce biti slican kao na slici LineCodeCoverage
