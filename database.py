import sqlite3
import datetime
import logging
from config import DB_NAME

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name
        self.init_db()
    
    def get_connection(self):
        """Создает соединение с БД"""
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_db(self):
        """Создает таблицы если их нет"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                nickname TEXT NOT NULL,
                district TEXT DEFAULT '🏛️ Центральный',
                anon_mode INTEGER DEFAULT 1,
                join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_chats INTEGER DEFAULT 0,
                total_messages INTEGER DEFAULT 0,
                district_chats INTEGER DEFAULT 0
            )
        ''')
        
        # Таблица рейтинга
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                likes INTEGER DEFAULT 0,
                dislikes INTEGER DEFAULT 0,
                rating REAL DEFAULT 50.0,
                banned INTEGER DEFAULT 0,
                ban_date TIMESTAMP,
                ban_reason TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица черного списка
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                blocked_id INTEGER NOT NULL,
                block_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, blocked_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (blocked_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица чатов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT UNIQUE NOT NULL,
                user1_id INTEGER NOT NULL,
                user2_id INTEGER NOT NULL,
                user1_nick TEXT NOT NULL,
                user2_nick TEXT NOT NULL,
                district TEXT,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                message_count INTEGER DEFAULT 0,
                FOREIGN KEY (user1_id) REFERENCES users (user_id),
                FOREIGN KEY (user2_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица сообщений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                from_user INTEGER NOT NULL,
                to_user INTEGER NOT NULL,
                from_nick TEXT NOT NULL,
                to_nick TEXT NOT NULL,
                message_text TEXT,
                message_type TEXT DEFAULT 'text',
                file_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chat_id) REFERENCES chats (chat_id),
                FOREIGN KEY (from_user) REFERENCES users (user_id),
                FOREIGN KEY (to_user) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица статистики по дням
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE UNIQUE NOT NULL,
                total_messages INTEGER DEFAULT 0,
                total_chats INTEGER DEFAULT 0,
                new_users INTEGER DEFAULT 0,
                active_users INTEGER DEFAULT 0
            )
        ''')
        
        # Таблица статистики по районам
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                district TEXT NOT NULL,
                user_count INTEGER DEFAULT 0,
                online_now INTEGER DEFAULT 0,
                UNIQUE(district)
            )
        ''')
        
        # Таблица для логов действий админов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                target_id INTEGER,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")
    
    # ===== ПОЛЬЗОВАТЕЛИ =====
    def add_user(self, user_id: int, nickname: str, district: str = "🏛️ Центральный"):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id, nickname, district, join_date, last_activity)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', (user_id, nickname, district))
            
            cursor.execute('''
                INSERT OR IGNORE INTO ratings (user_id, likes, dislikes, rating)
                VALUES (?, 0, 0, 50.0)
            ''', (user_id,))
            
            # Обновляем статистику районов
            cursor.execute('''
                INSERT INTO district_stats (district, user_count, online_now)
                VALUES (?, 1, 0)
                ON CONFLICT(district) DO UPDATE SET
                user_count = user_count + 1
            ''', (district,))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return False
        finally:
            conn.close()
    
    def get_user(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.*, r.likes, r.dislikes, r.rating, r.banned, r.ban_date
            FROM users u
            LEFT JOIN ratings r ON u.user_id = r.user_id
            WHERE u.user_id = ?
        ''', (user_id,))
        user = cursor.fetchone()
        conn.close()
        return user
    
    def update_user_district(self, user_id: int, new_district: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем старый район
        cursor.execute('SELECT district FROM users WHERE user_id = ?', (user_id,))
        old_district_row = cursor.fetchone()
        
        if old_district_row:
            old_district = old_district_row[0]
            # Уменьшаем счетчик в старом районе
            cursor.execute('''
                UPDATE district_stats SET user_count = user_count - 1
                WHERE district = ?
            ''', (old_district,))
        
        # Обновляем район пользователя
        cursor.execute('''
            UPDATE users SET district = ? WHERE user_id = ?
        ''', (new_district, user_id))
        
        # Увеличиваем счетчик в новом районе
        cursor.execute('''
            INSERT INTO district_stats (district, user_count, online_now)
            VALUES (?, 1, 0)
            ON CONFLICT(district) DO UPDATE SET
            user_count = user_count + 1
        ''', (new_district,))
        
        conn.commit()
        conn.close()
    
    def update_user_activity(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET last_activity = CURRENT_TIMESTAMP
            WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
    
    def update_nickname(self, user_id: int, new_nick: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET nickname = ? WHERE user_id = ?
        ''', (new_nick, user_id))
        conn.commit()
        conn.close()
    
    def toggle_anon_mode(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE users SET anon_mode = NOT anon_mode WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        conn.close()
    
    # ===== РЕЙТИНГ И БАНЫ =====
    def update_rating(self, user_id: int, is_like: bool):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if is_like:
            cursor.execute('''
                UPDATE ratings SET likes = likes + 1 WHERE user_id = ?
            ''', (user_id,))
        else:
            cursor.execute('''
                UPDATE ratings SET dislikes = dislikes + 1 WHERE user_id = ?
            ''', (user_id,))
        
        cursor.execute('''
            UPDATE ratings 
            SET rating = (likes * 100.0 / (likes + dislikes))
            WHERE user_id = ? AND (likes + dislikes) > 0
        ''', (user_id,))
        
        cursor.execute('''
            SELECT likes, dislikes, rating FROM ratings WHERE user_id = ?
        ''', (user_id,))
        data = cursor.fetchone()
        
        if data and data['dislikes'] >= 30 and data['rating'] < 50:
            cursor.execute('''
                UPDATE ratings SET banned = 1, ban_date = CURRENT_TIMESTAMP, ban_reason = 'Автоматический бан (30+ дизлайков)'
                WHERE user_id = ?
            ''', (user_id,))
            logger.info(f"User {user_id} banned automatically")
        
        conn.commit()
        conn.close()
    
    def check_banned(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT banned FROM ratings WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return bool(result and result['banned'] == 1)
    
    def ban_user(self, user_id: int, reason: str = "Нарушение правил"):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE ratings SET banned = 1, ban_date = CURRENT_TIMESTAMP, ban_reason = ?
            WHERE user_id = ?
        ''', (reason, user_id))
        conn.commit()
        conn.close()
    
    def unban_user(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE ratings SET banned = 0, ban_date = NULL, ban_reason = NULL WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
    
    def get_banned_users(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.user_id, u.nickname, u.district, r.likes, r.dislikes, r.rating, r.ban_date, r.ban_reason
            FROM users u
            JOIN ratings r ON u.user_id = r.user_id
            WHERE r.banned = 1
            ORDER BY r.ban_date DESC
        ''')
        users = cursor.fetchall()
        conn.close()
        return users
    
    def get_top_users(self, limit: int = 10):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.user_id, u.nickname, u.district, r.likes, r.dislikes, r.rating
            FROM users u
            JOIN ratings r ON u.user_id = r.user_id
            WHERE r.banned = 0 AND (r.likes + r.dislikes) > 0
            ORDER BY r.likes DESC, r.rating DESC
            LIMIT ?
        ''', (limit,))
        users = cursor.fetchall()
        conn.close()
        return users
    
    # ===== ЧЕРНЫЙ СПИСОК =====
    def add_to_blacklist(self, user_id: int, blocked_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO blacklist (user_id, blocked_id)
            VALUES (?, ?)
        ''', (user_id, blocked_id))
        conn.commit()
        conn.close()
    
    def remove_from_blacklist(self, user_id: int, blocked_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM blacklist WHERE user_id = ? AND blocked_id = ?
        ''', (user_id, blocked_id))
        conn.commit()
        conn.close()
    
    def get_blacklist(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT b.blocked_id, u.nickname, u.district, r.rating
            FROM blacklist b
            JOIN users u ON b.blocked_id = u.user_id
            LEFT JOIN ratings r ON b.blocked_id = r.user_id
            WHERE b.user_id = ?
        ''', (user_id,))
        blacklist = cursor.fetchall()
        conn.close()
        return blacklist
    
    def is_blocked(self, user_id: int, target_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM blacklist WHERE user_id = ? AND blocked_id = ?
        ''', (user_id, target_id))
        result = cursor.fetchone()
        conn.close()
        return bool(result)
    
    # ===== ЧАТЫ И СООБЩЕНИЯ =====
    def create_chat(self, chat_id: str, user1_id: int, user2_id: int, user1_nick: str, user2_nick: str, district: str = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO chats (chat_id, user1_id, user2_id, user1_nick, user2_nick, district, start_time)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (chat_id, user1_id, user2_id, user1_nick, user2_nick, district))
        
        cursor.execute('''
            UPDATE users SET total_chats = total_chats + 1
            WHERE user_id IN (?, ?)
        ''', (user1_id, user2_id))
        
        if district and district != 'разные районы':
            cursor.execute('''
                UPDATE users SET district_chats = district_chats + 1
                WHERE user_id IN (?, ?) AND district = ?
            ''', (user1_id, user2_id, district))
        
        chat_id_db = cursor.lastrowid
        conn.commit()
        conn.close()
        return chat_id_db
    
    def end_chat(self, chat_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE chats SET end_time = CURRENT_TIMESTAMP
            WHERE chat_id = ? AND end_time IS NULL
        ''', (chat_id,))
        conn.commit()
        conn.close()
    
    def save_message(self, chat_id: str, from_user: int, to_user: int, from_nick: str, to_nick: str, text: str = None, msg_type: str = "text", file_id: str = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO messages (chat_id, from_user, to_user, from_nick, to_nick, message_text, message_type, file_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (chat_id, from_user, to_user, from_nick, to_nick, text, msg_type, file_id))
        
        # Обновляем счетчик сообщений в чате
        cursor.execute('''
            UPDATE chats SET message_count = message_count + 1
            WHERE chat_id = ?
        ''', (chat_id,))
        
        # Обновляем счетчик сообщений пользователя
        cursor.execute('''
            UPDATE users SET total_messages = total_messages + 1
            WHERE user_id = ?
        ''', (from_user,))
        
        conn.commit()
        conn.close()
    
    def search_messages(self, search_text: str, limit: int = 50):
        """Поиск сообщений по тексту"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT m.*, c.user1_nick, c.user2_nick, c.start_time as chat_start
                FROM messages m
                JOIN chats c ON m.chat_id = c.chat_id
                WHERE m.message_text LIKE ?
                ORDER BY m.timestamp DESC
                LIMIT ?
            ''', (f'%{search_text}%', limit))
            messages = cursor.fetchall()
            return messages
        except Exception as e:
            logger.error(f"Error searching messages: {e}")
            return []
        finally:
            conn.close()
    
    def get_user_chats(self, user_id: int, limit: int = 20):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM chats 
            WHERE user1_id = ? OR user2_id = ?
            ORDER BY start_time DESC
            LIMIT ?
        ''', (user_id, user_id, limit))
        chats = cursor.fetchall()
        conn.close()
        return chats
    
    # ===== РАЙОНЫ И СТАТИСТИКА =====
    def get_district_stats(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT district, user_count, online_now 
            FROM district_stats 
            ORDER BY user_count DESC
        ''')
        stats = cursor.fetchall()
        conn.close()
        return stats
    
    def update_online_status(self, user_id: int, is_online: bool):
        """Обновляет онлайн статус пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем район пользователя
        cursor.execute('SELECT district FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            conn.close()
            return
        
        district = user[0]
        
        if is_online:
            cursor.execute('''
                UPDATE district_stats SET online_now = online_now + 1
                WHERE district = ?
            ''', (district,))
        else:
            cursor.execute('''
                UPDATE district_stats SET online_now = 
                    CASE WHEN online_now > 0 THEN online_now - 1 ELSE 0 END
                WHERE district = ?
            ''', (district,))
        
        conn.commit()
        conn.close()
    
    def get_users_by_district(self, district: str, exclude_user_id: int = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        if exclude_user_id:
            cursor.execute('''
                SELECT u.user_id, u.nickname, u.district, u.last_activity, 
                       u.total_chats, u.total_messages, r.likes, r.dislikes, r.rating, r.banned
                FROM users u
                LEFT JOIN ratings r ON u.user_id = r.user_id
                WHERE u.district = ? AND u.user_id != ? AND r.banned = 0
                ORDER BY u.last_activity DESC
            ''', (district, exclude_user_id))
        else:
            cursor.execute('''
                SELECT u.user_id, u.nickname, u.district, u.last_activity, 
                       u.total_chats, u.total_messages, r.likes, r.dislikes, r.rating, r.banned
                FROM users u
                LEFT JOIN ratings r ON u.user_id = r.user_id
                WHERE u.district = ?
                ORDER BY u.last_activity DESC
            ''', (district,))
        users = cursor.fetchall()
        conn.close()
        return users
    
    # ===== СТАТИСТИКА =====
    def update_daily_stats(self):
        today = datetime.datetime.now().date()
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR IGNORE INTO stats (date) VALUES (?)
        ''', (today,))
        
        cursor.execute('''
            SELECT COUNT(*) as count FROM users 
            WHERE DATE(join_date) = ?
        ''', (today,))
        new_users = cursor.fetchone()['count']
        
        cursor.execute('''
            SELECT COUNT(*) as count FROM users 
            WHERE DATE(last_activity) = ?
        ''', (today,))
        active_users = cursor.fetchone()['count']
        
        cursor.execute('''
            SELECT COUNT(*) as count FROM messages 
            WHERE DATE(timestamp) = ?
        ''', (today,))
        messages = cursor.fetchone()['count']
        
        cursor.execute('''
            SELECT COUNT(*) as count FROM chats 
            WHERE DATE(start_time) = ?
        ''', (today,))
        chats = cursor.fetchone()['count']
        
        cursor.execute('''
            UPDATE stats SET 
                total_messages = ?,
                total_chats = ?,
                new_users = ?,
                active_users = ?
            WHERE date = ?
        ''', (messages, chats, new_users, active_users, today))
        
        conn.commit()
        conn.close()
        
        return {
            'date': today,
            'messages': messages,
            'chats': chats,
            'new_users': new_users,
            'active_users': active_users
        }
    
    def get_all_stats(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) as count FROM users')
        total_users = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM users WHERE DATE(last_activity) = DATE("now")')
        active_today = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM messages')
        total_messages = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM chats')
        total_chats = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM ratings WHERE banned = 1')
        banned_users = cursor.fetchone()['count']
        
        cursor.execute('SELECT COUNT(*) as count FROM blacklist')
        total_blacklists = cursor.fetchone()['count']
        
        cursor.execute('''
            SELECT * FROM stats ORDER BY date DESC LIMIT 7
        ''')
        daily_stats = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_users': total_users,
            'active_today': active_today,
            'total_messages': total_messages,
            'total_chats': total_chats,
            'banned_users': banned_users,
            'total_blacklists': total_blacklists,
            'daily_stats': daily_stats
        }
    
    def get_user_details(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT u.*, r.likes, r.dislikes, r.rating, r.banned, r.ban_date, r.ban_reason
            FROM users u
            LEFT JOIN ratings r ON u.user_id = r.user_id
            WHERE u.user_id = ?
        ''', (user_id,))
        user = cursor.fetchone()
        
        if user:
            cursor.execute('''
                SELECT COUNT(*) as count FROM chats 
                WHERE user1_id = ? OR user2_id = ?
            ''', (user_id, user_id))
            chats_count = cursor.fetchone()['count']
            
            cursor.execute('''
                SELECT COUNT(*) as count FROM messages 
                WHERE from_user = ?
            ''', (user_id,))
            messages_count = cursor.fetchone()['count']
            
            cursor.execute('''
                SELECT COUNT(*) as count FROM blacklist WHERE user_id = ?
            ''', (user_id,))
            blacklist_count = cursor.fetchone()['count']
            
            cursor.execute('''
                SELECT COUNT(*) as count FROM blacklist WHERE blocked_id = ?
            ''', (user_id,))
            blocked_by_count = cursor.fetchone()['count']
            
            cursor.execute('''
                SELECT chat_id, start_time, message_count FROM chats 
                WHERE user1_id = ? OR user2_id = ?
                ORDER BY start_time DESC
                LIMIT 5
            ''', (user_id, user_id))
            recent_chats = cursor.fetchall()
            
            result = dict(user)
            result['total_chats'] = chats_count
            result['total_messages'] = messages_count
            result['blacklist_count'] = blacklist_count
            result['blocked_by_count'] = blocked_by_count
            result['recent_chats'] = recent_chats
            
            conn.close()
            return result
        
        conn.close()
        return None
    
    # ===== ЛОГИ АДМИНОВ =====
    def log_admin_action(self, admin_id: int, action: str, target_id: int = None, details: str = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO admin_logs (admin_id, action, target_id, details)
            VALUES (?, ?, ?, ?)
        ''', (admin_id, action, target_id, details))
        conn.commit()
        conn.close()
    
    def get_admin_logs(self, limit: int = 50):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM admin_logs 
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (limit,))
        logs = cursor.fetchall()
        conn.close()
        return logs