@echo.
@echo Command-line samples for PDF2Image
@echo Copyright 2014 PDFTron Systems Inc.
@echo.
@echo Example 1) Convert PDF to PNG using default resolution settings:
pdf2image -o test_out/ex1 test/tiger.pdf
@echo.
@echo Example 2) Convert PDF to PNG 100x100 pixel thumbnail; also rotate image 90 degrees couterclockwise:
pdf2image -o test_out/ex2 --hres 100 --vres 100 --rotate 90 test/tiger.pdf
@echo.
@echo Example 3) Convert a password protected PDF to multi-page TIFF using grayscale rendering only:
pdf2image -o test_out/ex3 -f tif --multipage --gray -p secret --rotate 270 --verb 3 test/blue_secret.pdf
@echo.
@echo Example 4) Render a small clip region at high DPI (3000 DPI):
pdf2image -o test_out/ex4 --dpi 3000 -f jpg -c 216,522,330,600 test/tiger.pdf
@echo.
@echo Example 5) Batch convert all PDF-s in 'test' folder to RGB, multi-page TIFF:
pdf2image -o test_out/ex5 --dpi 200 -f tif --multipage --pass secret --verb 3 test
@echo.
@echo Example 6) Batch convert all PDF-s in 'test' folder to JPEG thumbnails:
pdf2image -o test_out/ex6 --hres 100 --format jpg --pass secret --verb 3 --subfolders test
@echo.
@pause
