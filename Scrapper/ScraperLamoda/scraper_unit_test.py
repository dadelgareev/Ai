import unittest
from unittest.mock import patch, MagicMock
from scraper import LamodaScraper  # Импортируем ваш класс
import os

class TestLamodaScraper(unittest.TestCase):

    def setUp(self):
        """Настраиваем начальное состояние перед каждым тестом."""
        self.scraper = LamodaScraper()

    @patch('lamoda_scraper.requests.get')
    def test_fetch_page_success(self, mock_get):
        """Тест успешного получения страницы."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<html></html>'
        mock_get.return_value = mock_response

        result = self.scraper.fetch_page()
        self.assertEqual(result, '<html></html>')

    @patch('lamoda_scraper.requests.get')
    def test_fetch_page_failure(self, mock_get):
        """Тест обработки ошибки при запросе."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        mock_get.return_value = mock_response

        result = self.scraper.fetch_page()
        self.assertIsNone(result)

    @patch('lamoda_scraper.LamodaScraper.fetch_page')
    def test_get_full_width_elements(self, mock_fetch_page):
        """Тест извлечения элементов категории."""
        mock_fetch_page.return_value = """
        <div class="x-tree-view-catalog-navigation__category">
            <a class="x-link" href="/category">Category Name</a>
            <span class="x-tree-view-catalog-navigation__found">10</span>
        </div>
        """
        result = self.scraper.get_full_width_elements("test_url")
        expected = [{
            'category_name': 'Category Name',
            'category_url': '/category',
            'item_count': '10'
        }]
        self.assertEqual(result, expected)

    @patch('lamoda_scraper.LamodaScraper.fetch_page')
    def test_parse_count_pages(self, mock_fetch_page):
        """Тест извлечения количества страниц."""
        mock_fetch_page.return_value = '{"pages":5}'
        result = self.scraper.parse_count_pages()
        self.assertEqual(result, 5)

    @patch('lamoda_scraper.requests.get')
    def test_download_image_success(self, mock_get):
        """Тест успешного скачивания изображения."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"image_data"
        mock_get.return_value = mock_response

        save_dir = "test_images"
        image_name = "test.jpg"
        os.makedirs(save_dir, exist_ok=True)

        try:
            result = self.scraper.download_image("http://example.com/image.jpg", save_dir, image_name)
            self.assertTrue(os.path.exists(result))
        finally:
            os.remove(os.path.join(save_dir, image_name))
            os.rmdir(save_dir)

    @patch('lamoda_scraper.requests.get')
    def test_download_image_failure(self, mock_get):
        """Тест ошибки при скачивании изображения."""
        mock_get.side_effect = Exception("Download failed")

        result = self.scraper.download_image("http://example.com/image.jpg", "test_dir", "test.jpg")
        self.assertIsNone(result)

    def test_remove_duplicates(self):
        """Тест удаления дубликатов из списка ссылок."""
        links = ["http://example.com/1", "http://example.com/2", "http://example.com/1"]
        result = self.scraper.remove_duplicates(links)
        self.assertEqual(result, ["http://example.com/1", "http://example.com/2"])

    def test_find_duplicates(self):
        """Тест поиска дубликатов."""
        links = ["http://example.com/1", "http://example.com/2", "http://example.com/1"]
        result = self.scraper.find_duplicates(links)
        self.assertEqual(result, ["http://example.com/1"])

if __name__ == '__main__':
    unittest.main()
