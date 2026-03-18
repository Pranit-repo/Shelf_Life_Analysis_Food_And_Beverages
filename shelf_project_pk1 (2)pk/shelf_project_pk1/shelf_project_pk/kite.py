import tkinter as tk
import math
import random
import time

class MakarSankrantiFestival:
    def __init__(self, root):
        self.root = root
        self.root.title("Makar Sankranti Celebration")
        
        # Window Setup
        self.width = 900
        self.height = 700
        
        # Center the window
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        x_c = (screen_width // 2) - (self.width // 2)
        y_c = (screen_height // 2) - (self.height // 2)
        self.root.geometry(f"{self.width}x{self.height}+{x_c}+{y_c}")
        
        # Canvas
        self.canvas = tk.Canvas(root, width=self.width, height=self.height, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True)
        
        # Control Button
        self.btn_next = tk.Button(
            root, text="Next Tradition >>", command=self.next_scene,
            font=("Arial", 12, "bold"), bg="#FFD700", fg="#333", relief="raised"
        )
        self.btn_next.place(x=self.width - 160, y=self.height - 50)
        
        # State
        self.scene_index = 0
        self.animation_running = True
        self.kites = []
        self.steam_particles = []
        
        # Initialize First Scene
        self.load_scene(0)
        self.animate_loop()

    def load_scene(self, index):
        self.scene_index = index
        self.canvas.delete("all")
        self.kites = []
        self.steam_particles = []
        
        if index == 0:
            self.setup_kite_scene()
        elif index == 1:
            self.setup_food_scene()
        elif index == 2:
            self.setup_haldi_kumkum_scene()
        elif index == 3:
            self.setup_greeting_scene()

    def next_scene(self):
        self.scene_index = (self.scene_index + 1) % 4
        self.load_scene(self.scene_index)

    def animate_loop(self):
        if self.scene_index == 0:
            self.update_kites()
        elif self.scene_index == 1:
            self.update_steam()
            
        self.root.after(50, self.animate_loop)

    # ==========================
    # SCENE 1: KITE FLYING
    # ==========================
    def setup_kite_scene(self):
        # Sky Background
        self.canvas.create_rectangle(0, 0, self.width, self.height, fill="#87CEEB", width=0)
        
        # Sun
        self.canvas.create_oval(750, 50, 850, 150, fill="#FFD700", outline="#FFA500", width=2)
        
        # Clouds
        self.draw_cloud(100, 100)
        self.draw_cloud(400, 50)
        self.draw_cloud(600, 150)
        
        # Create Kites
        colors = ["#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#FFA500"]
        for _ in range(8):
            x = random.randint(50, self.width - 50)
            y = random.randint(50, 400)
            color = random.choice(colors)
            scale = random.uniform(0.5, 1.2)
            self.kites.append({"x": x, "y": y, "base_x": x, "color": color, "scale": scale, "phase": random.random() * 6.28})
            
        # Title
        self.canvas.create_text(self.width//2, self.height - 50, text="THE GREAT KITE FLYING COMPETITION", 
                                font=("Helvetica", 24, "bold"), fill="#333")
        self.canvas.create_text(self.width//2, self.height - 25, text="Kai Po Che!", 
                                font=("Helvetica", 16, "italic"), fill="#555")

    def draw_cloud(self, x, y):
        self.canvas.create_oval(x, y, x+60, y+40, fill="white", outline="")
        self.canvas.create_oval(x+30, y-10, x+90, y+40, fill="white", outline="")
        self.canvas.create_oval(x+60, y, x+120, y+40, fill="white", outline="")

    def update_kites(self):
        self.canvas.delete("kite")
        for k in self.kites:
            # Sway motion
            k["phase"] += 0.1
            sway_x = math.sin(k["phase"]) * 20
            sway_y = math.cos(k["phase"] * 0.5) * 10
            
            cx = k["base_x"] + sway_x
            cy = k["y"] + sway_y
            s = k["scale"]
            
            # Kite Coordinates (Diamond)
            # Center cx, cy
            points = [
                cx, cy - 40*s,  # Top
                cx + 30*s, cy,  # Right
                cx, cy + 40*s,  # Bottom
                cx - 30*s, cy   # Left
            ]
            
            # String (Manja)
            self.canvas.create_line(cx, cy + 40*s, cx - 100*s + sway_x, self.height, fill="white", width=1, tags="kite")
            
            # Tail
            self.canvas.create_polygon(
                cx, cy+40*s, cx-10*s, cy+60*s, cx+10*s, cy+60*s, 
                fill=k["color"], outline="black", tags="kite"
            )
            
            # Body
            self.canvas.create_polygon(points, fill=k["color"], outline="black", width=2, tags="kite")
            # Cross sticks
            self.canvas.create_line(cx, cy - 40*s, cx, cy + 40*s, fill="black", tags="kite")
            self.canvas.create_line(cx - 30*s, cy, cx + 30*s, cy, fill="black", tags="kite")

    # ==========================
    # SCENE 2: MIX VEG FEAST
    # ==========================
    def setup_food_scene(self):
        # Background: Tablecloth pattern
        self.canvas.create_rectangle(0, 0, self.width, self.height, fill="#FFF8DC", width=0)
        # Checkered pattern
        for i in range(0, self.width, 100):
            self.canvas.create_line(i, 0, i, self.height, fill="#DEB887", width=2)
        for i in range(0, self.height, 100):
            self.canvas.create_line(0, i, self.width, i, fill="#DEB887", width=2)

        cx, cy = self.width // 2, self.height // 2 + 50
        
        # Shadow
        self.canvas.create_oval(cx - 220, cy + 120, cx + 220, cy + 180, fill="#CD853F", outline="")

        # Clay Pot / Handi
        # Rim
        self.canvas.create_oval(cx - 200, cy - 100, cx + 200, cy + 100, fill="#8B4513", outline="black", width=3)
        # Body (Arc)
        self.canvas.create_arc(cx - 200, cy - 100, cx + 200, cy + 150, start=180, extent=180, fill="#A0522D", outline="black", width=3)
        
        # The Mix Vegetable Dish (Bhogi chi Bhaji / Undhiyu) content
        # Fill the oval with 'gravy'
        self.canvas.create_oval(cx - 180, cy - 80, cx + 180, cy + 80, fill="#556B2F", outline="") # Dark olive green gravy
        
        # Draw ingredients
        self.draw_ingredient(cx, cy, 30, "purple") # Brinjal
        self.draw_ingredient(cx-50, cy-20, 25, "orange") # Carrot
        self.draw_ingredient(cx+60, cy+30, 25, "green") # Beans/Peas
        self.draw_ingredient(cx-30, cy+50, 30, "#CD853F") # Potato/Yam
        self.draw_ingredient(cx+40, cy-40, 20, "darkred") # Ber (Jujube)
        self.draw_ingredient(cx-80, cy+10, 20, "purple")
        self.draw_ingredient(cx+90, cy-10, 25, "green")
        
        # Sesame Seeds garnish (White dots)
        for _ in range(50):
            sx = random.randint(cx - 150, cx + 150)
            sy = random.randint(cy - 60, cy + 60)
            self.canvas.create_oval(sx, sy, sx+3, sy+4, fill="white", outline="")
            
        # Coriander garnish
        self.canvas.create_arc(cx, cy, cx+20, cy+20, start=0, extent=120, style=tk.ARC, outline="#32CD32", width=3)
        self.canvas.create_arc(cx-40, cy-30, cx-20, cy-10, start=0, extent=120, style=tk.ARC, outline="#32CD32", width=3)

        # Labels
        self.canvas.create_text(self.width//2, 100, text="TRADITIONAL WINTER FEAST", 
                                font=("Times New Roman", 30, "bold"), fill="#8B4513")
        self.canvas.create_text(self.width//2, 150, text="Bhogi chi Bhaji / Undhiyu", 
                                font=("Times New Roman", 20, "italic"), fill="#556B2F")
        self.canvas.create_text(self.width//2, self.height - 80, text="(Mixed Seasonal Vegetables with Sesame)", 
                                font=("Arial", 14), fill="#333")

    def draw_ingredient(self, cx, cy, r, color):
        # Helper to draw random veg chunks inside the pot
        x = cx + random.randint(-80, 80)
        y = cy + random.randint(-40, 40)
        self.canvas.create_oval(x-r, y-r, x+r, y+r, fill=color, outline="#333", width=1)

    def update_steam(self):
        cx, cy = self.width // 2, self.height // 2
        # Add steam particle
        if random.random() < 0.2:
            self.steam_particles.append({"x": cx + random.randint(-100, 100), "y": cy - 50, "life": 1.0})
            
        self.canvas.delete("steam")
        keep = []
        for p in self.steam_particles:
            p["y"] -= 2
            p["life"] -= 0.02
            if p["life"] > 0:
                # Draw translucent-ish steam (using stipple or gray)
                color = "#EEEEEE"
                # Simulating fade with size reduction
                r = 10 * p["life"]
                self.canvas.create_oval(p["x"]-r, p["y"]-r, p["x"]+r, p["y"]+r, fill=color, outline="", tags="steam")
                keep.append(p)
        self.steam_particles = keep

    # ==========================
    # SCENE 3: HALDI KUMKUM
    # ==========================
    def setup_haldi_kumkum_scene(self):
        self.canvas.create_rectangle(0, 0, self.width, self.height, fill="#FFFAF0", width=0)
        
        cx, cy = self.width // 2, self.height // 2
        
        # Thali (Plate)
        self.canvas.create_oval(cx - 200, cy - 200, cx + 200, cy + 200, fill="#C0C0C0", outline="#A9A9A9", width=5)
        # Inner rim
        self.canvas.create_oval(cx - 180, cy - 180, cx + 180, cy + 180, outline="#A9A9A9", width=2)
        
        # Haldi Bowl (Turmeric - Yellow)
        hx, hy = cx - 80, cy
        self.canvas.create_oval(hx - 50, hy - 50, hx + 50, hy + 50, fill="silver", outline="gray", width=2)
        self.canvas.create_oval(hx - 40, hy - 40, hx + 40, hy + 40, fill="#FFD700", outline="#DAA520") # Powder
        self.canvas.create_text(hx, hy + 70, text="Haldi", font=("Arial", 12, "bold"), fill="#B8860B")

        # Kumkum Bowl (Vermilion - Red)
        kx, ky = cx + 80, cy
        self.canvas.create_oval(kx - 50, ky - 50, kx + 50, ky + 50, fill="silver", outline="gray", width=2)
        self.canvas.create_oval(kx - 40, ky - 40, kx + 40, ky + 40, fill="#FF0000", outline="#8B0000") # Powder
        self.canvas.create_text(kx, ky + 70, text="Kumkum", font=("Arial", 12, "bold"), fill="#8B0000")

        # Flowers
        self.draw_flower(cx, cy - 120, "orange")
        self.draw_flower(cx, cy + 120, "yellow")
        
        # Diya (Lamp)
        dx, dy = cx, cy + 250
        self.canvas.create_oval(dx - 30, dy, dx + 30, dy + 30, fill="#8B4513", outline="")
        self.canvas.create_polygon(dx - 30, dy + 15, dx + 30, dy + 15, dx, dy + 50, fill="#8B4513", outline="")
        # Flame
        self.canvas.create_polygon(dx - 10, dy + 5, dx + 10, dy + 5, dx, dy - 30, fill="orange", outline="yellow", width=2)
        
        # Title
        self.canvas.create_text(self.width//2, 80, text="LADIES' SPECIAL TRADITION", 
                                font=("Georgia", 30, "bold"), fill="#C71585")
        self.canvas.create_text(self.width//2, 130, text="Haldi Kumkum Ceremony", 
                                font=("Georgia", 20), fill="#333")
        
        # Vaan (Gift) Symbol
        self.canvas.create_rectangle(cx - 250, cy + 180, cx - 150, cy + 280, fill="#FF69B4", outline="black")
        self.canvas.create_line(cx - 200, cy + 180, cx - 200, cy + 280, fill="gold", width=5)
        self.canvas.create_line(cx - 250, cy + 230, cx - 150, cy + 230, fill="gold", width=5)
        self.canvas.create_text(cx - 200, cy + 300, text="Vaan (Gift)", font=("Arial", 10))

    def draw_flower(self, x, y, color):
        for i in range(0, 360, 45):
            rad = math.radians(i)
            ox = x + math.cos(rad) * 20
            oy = y + math.sin(rad) * 20
            self.canvas.create_oval(ox-10, oy-10, ox+10, oy+10, fill=color, outline="")
        self.canvas.create_oval(x-10, y-10, x+10, y+10, fill="white", outline="")

    # ==========================
    # SCENE 4: GREETING
    # ==========================
    def setup_greeting_scene(self):
        self.canvas.create_rectangle(0, 0, self.width, self.height, fill="#FFDAB9", width=0)
        
        # Text
        self.canvas.create_text(self.width//2, self.height//2 - 50, text="HAPPY MAKAR SANKRANTI", 
                                font=("Arial", 40, "bold"), fill="#FF4500")
        
        self.canvas.create_text(self.width//2, self.height//2 + 50, text="Tilgul Ghya, God God Bola", 
                                font=("Arial", 24, "italic"), fill="#333")
                                
        # Draw Tilgul Ladoos (Sesame Sweets)
        for i in range(5):
            x = (self.width // 2 - 100) + i * 50
            y = self.height // 2 + 120
            self.canvas.create_oval(x-20, y-20, x+20, y+20, fill="#FFF8DC", outline="#D2691E", width=2)
            # Dots
            for _ in range(10):
                dx = x + random.randint(-10, 10)
                dy = y + random.randint(-10, 10)
                self.canvas.create_line(dx, dy, dx+1, dy+1, fill="black")

if __name__ == "__main__":
    root = tk.Tk()
    app = MakarSankrantiFestival(root)
    root.mainloop()